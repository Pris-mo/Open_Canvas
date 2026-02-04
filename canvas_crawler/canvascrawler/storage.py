import os
import json
import requests
import zipfile
import hashlib
import re


class StorageManager:
    def __init__(self, base_dir, logger):
        self.base_dir = base_dir
        self.logger   = logger

    def extract_zip_recursive_using_parent_json(
        self,
        zip_abs_path: str,
        parent_record: dict,
        max_depth: int = 8,
        max_members: int = 50000,
        max_total_uncompressed: int = 5 * 1024**3,
    ):
        """
        Extract zip (and nested zips) under files/<parent_id>__zip/
        and emit JSON per extracted file by cloning parent_record and
        overwriting title/extension/raw_file_path.
        """
        parent_id = str(parent_record.get("id") or "unknown")
        root_out = os.path.join(self.base_dir, "files", f"{parent_id}__zip")
        os.makedirs(root_out, exist_ok=True)

        stack = [(zip_abs_path, root_out, 0, os.path.basename(zip_abs_path))]
        seen = set()

        while stack:
            cur_zip, cur_out_root, depth, parent_zip_name = stack.pop()

            if depth > max_depth:
                self.logger.warning(f"Zip recursion depth exceeded ({max_depth}): {cur_zip}")
                continue

            real = os.path.realpath(cur_zip)
            if real in seen:
                continue
            seen.add(real)

            if not zipfile.is_zipfile(cur_zip):
                self.logger.warning(f"Not a valid zip, skipping: {cur_zip}")
                continue

            self.logger.info(f"Extracting zip: {cur_zip} -> {cur_out_root} (depth={depth})")

            total_uncompressed = 0
            members = 0

            with zipfile.ZipFile(cur_zip, "r") as zf:
                for info in zf.infolist():
                    if info.is_dir():
                        continue

                    members += 1
                    if members > max_members:
                        self.logger.warning(f"Zip member limit exceeded ({max_members}) in {cur_zip}")
                        break

                    file_size = int(getattr(info, "file_size", 0) or 0)
                    total_uncompressed += file_size
                    if total_uncompressed > max_total_uncompressed:
                        self.logger.warning(f"Zip uncompressed limit exceeded in {cur_zip}")
                        break

                    member_rel = self._sanitize_zip_member_path(info.filename)
                    if not member_rel:
                        continue

                    # destination path under current out root
                    dest_path = os.path.join(cur_out_root, member_rel)
                    dest_path = self._dedupe_path(dest_path)  # add (n) on collision
                    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

                    # ZipSlip protection: ensure dest stays within cur_out_root
                    if not os.path.realpath(dest_path).startswith(os.path.realpath(cur_out_root) + os.sep):
                        self.logger.warning(f"Blocked ZipSlip path: {info.filename}")
                        continue

                    # extract by streaming; also compute sha256 if you want
                    sha = hashlib.sha256()
                    with zf.open(info, "r") as src, open(dest_path, "wb") as out:
                        for chunk in iter(lambda: src.read(1024 * 1024), b""):
                            out.write(chunk)
                            sha.update(chunk)

                    # build cloned JSON record
                    cloned = dict(parent_record)  # shallow clone is fine
                    extracted_filename = os.path.basename(dest_path)
                    ext = extracted_filename.split(".")[-1].lower() if "." in extracted_filename else ""

                    # raw_file_path should be relative to base_dir like your other records
                    rel_raw = os.path.relpath(dest_path, self.base_dir).replace("\\", "/")

                    cloned["title"] = extracted_filename
                    cloned["extension"] = ext
                    cloned["raw_file_path"] = rel_raw

                    # Optional provenance fields (highly recommended):
                    cloned["zip_member_path"] = member_rel.replace("\\", "/")
                    cloned["zip_parent"] = parent_zip_name
                    cloned["zip_depth"] = depth
                    cloned["sha256"] = sha.hexdigest()

                    # Write JSON next to your json_output in a mirrored structure
                    json_rel = os.path.join(
                        "json_output",
                        "files",
                        f"{parent_id}__zip",
                        member_rel + ".json"
                    ).replace("\\", "/")

                    # If we deduped the file name, the JSON path should match that final name
                    # Replace trailing filename component in json_rel with the deduped filename
                    json_rel = self._adjust_json_rel_for_dedup(json_rel, dest_path, cur_out_root, parent_id)

                    self.write_json_path(cloned, json_rel)

                    # if extracted member is a zip, recurse
                    if dest_path.lower().endswith(".zip") and zipfile.is_zipfile(dest_path):
                        nested_out_root = os.path.splitext(dest_path)[0] + "__zip"
                        os.makedirs(nested_out_root, exist_ok=True)
                        stack.append((dest_path, nested_out_root, depth + 1, os.path.basename(dest_path)))


    def _sanitize_zip_member_path(self, name: str):
        name = name.replace("\\", "/").strip()

        # strip drive letters like C:
        if len(name) >= 2 and name[1] == ":":
            name = name[2:]

        name = name.lstrip("/")
        if not name:
            return None

        parts = []
        for part in name.split("/"):
            if part in ("", "."):
                continue
            if part == "..":
                if parts:
                    parts.pop()
                continue
            parts.append(part)

        if not parts:
            return None
        return "/".join(parts)


    def _dedupe_path(self, path: str) -> str:
        """
        If path exists, append (n) before extension: file(1).pdf, file(2).pdf...
        """
        if not os.path.exists(path):
            return path

        base, ext = os.path.splitext(path)
        n = 1
        while True:
            cand = f"{base}({n}){ext}"
            if not os.path.exists(cand):
                return cand
            n += 1


    def _adjust_json_rel_for_dedup(self, json_rel: str, dest_path: str, cur_out_root: str, parent_id: str) -> str:
        """
        Ensure the JSON filename matches the possibly deduped extracted filename.
        We mirror the extracted relative path under files/<parent_id>__zip/...
        into json_output/files/<parent_id>__zip/... + '.json'
        """
        # relative path from base_dir
        rel_raw = os.path.relpath(dest_path, self.base_dir).replace("\\", "/")
        # expect rel_raw like: files/<parent_id>__zip/...
        if not rel_raw.startswith(f"files/{parent_id}__zip/"):
            return json_rel

        member_rel = rel_raw.split(f"files/{parent_id}__zip/", 1)[1]
        return f"json_output/files/{parent_id}__zip/{member_rel}.json"


    def write_json(self, record):
        # Use type + id if present, else just type
        suffix = f"_{record['id']}" if "id" in record else ""
        filename = f"{record['type']}{suffix}.json"
        
        path = os.path.join(self.base_dir, "json_output", filename)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2)
        self.logger.debug(f"Wrote JSON -> {path}")

    def write_json_path(self, record, json_rel_path):
        """
        Write JSON to an explicit relative path under base_dir.
        Example json_rel_path:
        json_output/files/805430__zip/foo/bar.pdf.json
        """
        path = os.path.join(self.base_dir, json_rel_path.lstrip("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2)
        self.logger.debug(f"Wrote JSON -> {path}")
        return path


    def download_file(self, url, file_path):
        full_path = os.path.join(self.base_dir, file_path.lstrip("/"))
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(full_path, "wb") as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
        self.logger.debug(f"Downloaded file -> {full_path}")
        return full_path  


    def write_html(self, content, file_path):
        path = os.path.join(self.base_dir, file_path.lstrip("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        self.logger.debug(f"Wrote HTML -> {path}")
