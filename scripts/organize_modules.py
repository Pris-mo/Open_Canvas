#!/usr/bin/env python3
"""
Reorganize Canvas export run directory into module-based folders and
update metadata JSON files plus an index CSV.

Expected layout (relative to run root, e.g. 20260217_085041):

- canvas/output/<course_id>/
    - json_output/
        - *.json        (metadata files)
    - quizzes/, assignments/, files/, pages/, ...

- chunker/chunks/
    - assignment_<id>/
    - quiz_<id>/
    - page_<id>/
    - file_<id>/
    ...

- filters/removed.jsonl   (optional; used to mark items to remove)

This script will create:

- Modules/
    - <module_name path>/   (split module_name on "/" into nested dirs)
        - <id>/
            - Json/<original_json_filename>.json
            - Raw/<raw file>
            - Chunks/<chunk files>

Each metadata JSON is updated to include:

- "raw_file_path": updated path to moved raw file (relative to run root)
- "chunk_paths": list of chunk file paths (relative to run root)
- "json_path": path to the JSON file itself (relative to run root)

A CSV index is also written (by default: Modules/modules_index.csv)
with columns:

Title, URL, Type, Module, Published?, File Path, Json Path,
Chunk Path, Remove?, Remove Reason?
"""

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import shutil


def infer_type(url: str, meta_type: Optional[str] = None) -> Optional[str]:
    """Infer Canvas object type from URL or meta_type.

    Returns one of: 'assignment', 'quiz', 'page', 'file', or None.
    """
    url = url or ""
    url_lower = url.lower()

    if "/assignments/" in url_lower:
        return "assignment"
    if "/quizzes/" in url_lower:
        return "quiz"
    if "/files/" in url_lower:
        return "file"
    if "/pages/" in url_lower:
        return "page"

    if meta_type:
        t = meta_type.lower()
        if t in {"assignment", "quiz", "page", "file"}:
            return t

    return None


def module_dir_for(modules_root: Path, module_name: Optional[str]) -> Path:
    """Compute the module directory for a given module_name.

    If module_name is None/empty, returns Modules/No_Module_Found.
    Otherwise, splits on "/" to create nested directories.
    """
    if not module_name:
        return modules_root / "No_Module_Found"

    # Split on "/" and strip whitespace from each part.
    parts = [p.strip() for p in str(module_name).split("/") if p.strip()]
    if not parts:
        return modules_root / "No_Module_Found"

    path = modules_root
    for part in parts:
        path = path / part
    return path


def rel_to_root(path: Path, root: Path) -> str:
    """Return POSIX-style relative path from root to path."""
    return path.relative_to(root).as_posix()

def load_removed_map(removed_path: Path) -> Dict[str, Dict[str, Any]]:
    """Load /filters/removed.jsonl into a lookup map.

    Keys are strings (normalized paths), e.g.:
        - "json_output/assignment_14113659.json"
        - "assignments/14113659.html"

    Values:
        {'remove': True, 'reason': '<reason plus extra context>'}

    If removed.jsonl does not exist, returns empty dict.
    """

    def norm_key(s: str) -> str:
        # Normalize to POSIX-ish, strip leading "./"
        return s.replace("\\", "/").lstrip("./")

    mapping: Dict[str, Dict[str, Any]] = {}

    if not removed_path.is_file():
        print(f"[info] No removed file found at {removed_path}, skipping removal info.", file=sys.stderr)
        return mapping

    with removed_path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[warn] Skipping invalid JSON on line {lineno} of {removed_path}: {e}", file=sys.stderr)
                continue

            # Only care about entries that actually mark something as removed
            action = data.get("action")
            if action not in {"removed", "remove"}:
                continue

            base_reason = data.get("reason", "")

            # Enrich the reason with extra info like blacklisted_term / dupe_of / token_count
            extra_bits = []
            if "blacklisted_term" in data:
                extra_bits.append(f"blacklist={data['blacklisted_term']}")
            if "dupe_of" in data:
                extra_bits.append(f"dupe_of={data['dupe_of']}")
            if "token_count" in data:
                extra_bits.append(f"tokens={data['token_count']}")

            if extra_bits:
                reason = f"{base_reason} ({', '.join(extra_bits)})"
            else:
                reason = base_reason

            entry = {"remove": True, "reason": reason}

            # Key by json_path if present
            json_path = data.get("json_path")
            if isinstance(json_path, str) and json_path:
                key = norm_key(json_path)
                mapping[key] = entry

            # Also key by raw content path if present
            path = data.get("path")
            if isinstance(path, str) and path:
                key = norm_key(path)
                mapping[key] = entry

    print(f"[info] Loaded {len(mapping)} removal entries from {removed_path}", file=sys.stderr)
    return mapping


def autodetect_course_dir(canvas_output_root: Path) -> Path:
    """If course_id is not given, try to auto-detect a single course dir."""
    if not canvas_output_root.is_dir():
        raise SystemExit(f"canvas/output directory not found at {canvas_output_root}")

    candidates = [p for p in canvas_output_root.iterdir() if p.is_dir()]
    if not candidates:
        raise SystemExit(f"No course directories found under {canvas_output_root}")
    if len(candidates) > 1:
        names = ", ".join(p.name for p in candidates)
        raise SystemExit(
            f"Multiple course directories found under {canvas_output_root}: {names}\n"
            f"Please specify --course-id explicitly."
        )
    return candidates[0]


def collect_json_files(json_output_dir: Path) -> List[Path]:
    """Recursively collect all JSON files under json_output_dir."""
    if not json_output_dir.is_dir():
        raise SystemExit(f"json_output directory not found at {json_output_dir}")
    return sorted(json_output_dir.rglob("*.json"))

def process_run(
    run_root: Path,
    course_id: Optional[str],
    removed_path: Path,
    csv_output_path: Optional[Path] = None,
    dry_run: bool = False,
) -> None:
    """Main processing function."""
    canvas_output_root = run_root / "canvas" / "output"
    if course_id:
        course_dir = canvas_output_root / str(course_id)
    else:
        course_dir = autodetect_course_dir(canvas_output_root)

    if not course_dir.is_dir():
        raise SystemExit(f"Course directory not found: {course_dir}")

    json_output_dir = course_dir / "json_output"
    json_files = collect_json_files(json_output_dir)
    if not json_files:
        print(f"[info] No JSON files found under {json_output_dir}", file=sys.stderr)
        return

    chunk_root = run_root / "chunker" / "chunks"
    modules_root = run_root / "Modules"
    modules_root.mkdir(parents=True, exist_ok=True)

    if not removed_path.is_absolute():
        removed_path = (run_root / removed_path).resolve()
    removed_map = load_removed_map(removed_path)

    # Determine where to write the CSV
    if csv_output_path is None:
        csv_output_path = modules_root / "modules_index.csv"
    else:
        if not csv_output_path.is_absolute():
            csv_output_path = (run_root / csv_output_path).resolve()

    csv_rows: List[Dict[str, Any]] = []

    for json_file in json_files:
        with json_file.open("r", encoding="utf-8") as f:
            try:
                meta = json.load(f)
            except json.JSONDecodeError as e:
                print(f"[warn] Skipping invalid JSON file {json_file}: {e}", file=sys.stderr)
                continue

        # Original paths for removal lookup (before we mutate meta or move files)
        orig_json_rel = json_file.relative_to(course_dir).as_posix()  # e.g. 'json_output/assignment_14113659.json'
        orig_raw_rel = meta.get("raw_file_path") or ""                # e.g. 'assignments/14113659.html'

        object_id = meta.get("id")
        if object_id is None:
            print(f"[warn] JSON file {json_file} missing 'id'; skipping.", file=sys.stderr)
            continue
        object_id_str = str(object_id)

        url = meta.get("url") or ""
        meta_type = meta.get("type")
        record_type = infer_type(url, meta_type)
        if record_type is None and meta_type:
            record_type = meta_type.lower()

        module_name = meta.get("module_name")

        # --- Determine removal info from removed_map (using original paths) ---
        def _norm_key(s: str) -> str:
            return s.replace("\\", "/").lstrip("./")

        removal_info = None
        if orig_json_rel:
            removal_info = removed_map.get(_norm_key(orig_json_rel))
        if removal_info is None and orig_raw_rel:
            removal_info = removed_map.get(_norm_key(orig_raw_rel))

        remove_flag = bool(removal_info["remove"]) if removal_info else False
        remove_reason = removal_info["reason"] if removal_info else ""

        # --- Build module/id dirs ---
        module_dir = module_dir_for(modules_root, module_name)
        id_dir = module_dir / object_id_str

        json_target_dir = id_dir / "Json"
        raw_target_dir = id_dir / "Raw"
        chunks_target_dir = id_dir / "Chunks"

        if not dry_run:
            json_target_dir.mkdir(parents=True, exist_ok=True)
            raw_target_dir.mkdir(parents=True, exist_ok=True)
            chunks_target_dir.mkdir(parents=True, exist_ok=True)

        # --- Move/copy raw file if we have a path ---
        raw_file_rel = meta.get("raw_file_path")
        raw_file_new_rel: str = ""
        if raw_file_rel:
            raw_src = course_dir / raw_file_rel
            if raw_src.is_file():
                raw_dst = raw_target_dir / raw_src.name
                if dry_run:
                    print(f"[dry-run] Would move raw file {raw_src} -> {raw_dst}", file=sys.stderr)
                else:
                    raw_dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(raw_src), str(raw_dst))
                raw_file_new_rel = rel_to_root(raw_dst, run_root)
                meta["raw_file_path"] = raw_file_new_rel
            else:
                print(
                    f"[warn] raw_file_path {raw_src} not found for id {object_id_str}; leaving as-is.",
                    file=sys.stderr,
                )
        else:
            # No raw_file_path in metadata; leave it unset
            pass

        # --- Move/copy chunk files if we can find a chunk directory ---
        chunk_paths: List[str] = []
        if record_type:
            chunk_src_dir = chunk_root / f"{record_type}_{object_id_str}"
            if chunk_src_dir.is_dir():
                for path in sorted(chunk_src_dir.rglob("*")):
                    if not path.is_file():
                        continue
                    rel_subpath = path.relative_to(chunk_src_dir)
                    chunk_dst = chunks_target_dir / rel_subpath
                    if dry_run:
                        print(f"[dry-run] Would move chunk file {path} -> {chunk_dst}", file=sys.stderr)
                    else:
                        chunk_dst.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(path), str(chunk_dst))
                    chunk_paths.append(rel_to_root(chunk_dst, run_root))
            else:
                # No chunk dir; fine
                pass

        meta["chunk_paths"] = chunk_paths
        meta["removed"] = remove_flag
        meta["remove_reason"] = remove_reason

        # --- Compute new JSON path and write updated JSON ---
        json_dst = json_target_dir / json_file.name
        json_path_rel = rel_to_root(json_dst, run_root)
        meta["json_path"] = json_path_rel

        if dry_run:
            print(f"[dry-run] Would write updated JSON for id {object_id_str} to {json_dst}", file=sys.stderr)
        else:
            with json_dst.open("w", encoding="utf-8") as out_f:
                json.dump(meta, out_f, indent=2, ensure_ascii=False)
                out_f.write("\n")

            # Remove original JSON file after moving
            try:
                json_file.unlink()
            except OSError as e:
                print(f"[warn] Could not delete original JSON file {json_file}: {e}", file=sys.stderr)

        # --- Prepare CSV row ---
        csv_rows.append(
            {
                "Title": meta.get("title", ""),
                "URL": url,
                "Type": record_type or (meta_type or ""),
                "Module": module_name or "No_Module_Found",
                "Published?": "TRUE" if bool(meta.get("published")) else "FALSE",
                "File Path": meta.get("raw_file_path", "") or "",
                "Json Path": json_path_rel,
                "Chunk Path": "|".join(chunk_paths),
                "Remove?": "TRUE" if remove_flag else "FALSE",
                "Remove Reason?": remove_reason,
            }
        )

    # --- Write CSV ---
    if dry_run:
        print(f"[dry-run] Would write CSV index to {csv_output_path}", file=sys.stderr)
    else:
        csv_output_path.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = [
            "Title",
            "URL",
            "Type",
            "Module",
            "Published?",
            "File Path",
            "Json Path",
            "Chunk Path",
            "Remove?",
            "Remove Reason?",
        ]
        with csv_output_path.open("w", encoding="utf-8", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in csv_rows:
                writer.writerow(row)
        print(f"[info] Wrote CSV index with {len(csv_rows)} rows to {csv_output_path}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reorganize Canvas export run directory into module folders and create an index CSV."
    )
    parser.add_argument(
        "run_root",
        nargs="?",
        default=".",
        help="Path to the run directory (e.g. ~/Applications/Open_Canvas/runs/20260217_085041). Default: current directory.",
    )
    parser.add_argument(
        "--course-id",
        help="Canvas course ID (e.g., 1899011). If omitted, will auto-detect if exactly one course dir exists under canvas/output.",
    )
    parser.add_argument(
        "--removed-file",
        default="filters/removed.jsonl",
        help="Path to removed.jsonl (absolute or relative to run_root). Default: filters/removed.jsonl",
    )
    parser.add_argument(
        "--csv-output",
        default=None,
        help="Path for the output CSV file (absolute or relative to run_root). Default: Modules/modules_index.csv",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not move or write any files; just print what would be done.",
    )

    args = parser.parse_args()
    run_root = Path(args.run_root).resolve()
    removed_path = Path(args.removed_file)

    process_run(
        run_root=run_root,
        course_id=args.course_id,
        removed_path=removed_path,
        csv_output_path=Path(args.csv_output) if args.csv_output else None,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
