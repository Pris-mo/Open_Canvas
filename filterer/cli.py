from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Optional, Dict, Any, Iterable


_TEXT_SUFFIXES = {".html", ".htm", ".txt", ".md", ".json"}

# Default blacklist
_DEFAULT_TITLE_BLACKLIST = {
    "midterm", "exam", "solution", "sol", "explanation", 
    "key", "answers","check-in","checkin","check in",
}

def _estimate_token_count(text: str) -> int:
    """
    Very rough token count: split on non-whitespace.
    If you later want model-accurate tokens, you can plug tiktoken in here.
    """
    return len(re.findall(r"\S+", text))

def _annotate_json_removal(
    *,
    course_root: Path,
    json_rel_str: str,
    meta: Dict[str, Any],
    event: Dict[str, Any],
) -> None:
    """
    Mark the JSON item corresponding to this file as removed.

    - Finds the entry with matching raw_file_path
    - Sets `removed = True`
    - Sets `removal_reason` from event["reason"]
    - Copies some extra event fields (token_count, suffix, blacklisted_term, dupe_of, etc.)
    """
    json_path = (course_root / json_rel_str).resolve()
    if not json_path.exists():
        return

    try:
        raw = json_path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except Exception:
        # Don't kill the run if a JSON can't be read/parsed
        return

    raw_file_path = meta.get("raw_file_path")
    if not isinstance(raw_file_path, str):
        return

    # Which event fields do we want to propagate into the item?
    extra_keys = ("token_count", "suffix", "blacklisted_term", "dupe_of")

    def mark_item(item: Dict[str, Any]) -> None:
        if not isinstance(item, dict):
            return
        if item.get("raw_file_path") != raw_file_path:
            return

        item["removed"] = True
        item["removal_reason"] = event.get("reason")

        # Copy over some extra metadata fields from the event
        for key in extra_keys:
            if key in event:
                item[f"removal_{key}"] = event[key]

    # Possible shapes:
    # 1) Top-level list of items
    if isinstance(data, list):
        for obj in data:
            if isinstance(obj, dict):
                mark_item(obj)

    # 2) Top-level dict
    elif isinstance(data, dict):
        # a) Single item dict with raw_file_path
        if "raw_file_path" in data:
            mark_item(data)

        # b) Container dict with "items" list
        items = data.get("items")
        if isinstance(items, list):
            for obj in items:
                if isinstance(obj, dict):
                    mark_item(obj)

    # Write the updated JSON back
    try:
        json_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        # If writing fails, just continue
        return


def _read_exclusion_csv(course_root: Path, csv_path: Path) -> set[Path]:
    """
    Load a CSV of files to exclude.

    Expected columns (any of these):
      - raw_file_path (preferred)
      - file_path
      - path

    Paths should be relative to course_root, e.g. 'pages/120231.html'.
    """
    csv_path = csv_path.resolve()
    if not csv_path.exists():
        raise FileNotFoundError(f"Exclusion CSV not found: {csv_path}")

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = [fn.strip() for fn in (reader.fieldnames or [])]

        path_key: Optional[str] = None
        for candidate in ("raw_file_path", "file_path", "path"):
            if candidate in fieldnames:
                path_key = candidate
                break

        if path_key is None:
            # fallback to the first column
            f.seek(0)
            first_line = next(f).strip()
            if not first_line:
                return set()
            headers = [h.strip() for h in first_line.split(",") if h.strip()]
            if not headers:
                return set()
            path_key = headers[0]
            f.seek(0)
            reader = csv.DictReader(f)

        rel_paths: set[Path] = set()
        for row in reader:
            raw_val = (row.get(path_key) or "").strip()
            if not raw_val:
                continue
            rel_paths.add(Path(raw_val))

    return {(course_root / p).resolve() for p in rel_paths}


def _should_skip(path: Path, root: Path, skip_dirnames: set[str]) -> bool:
    rel = path.relative_to(root)
    return any(part in skip_dirnames for part in rel.parts)


def _discover_files_under(
    root: Path,
    *,
    skip_dirnames: set[str],
    include_dirnames: Optional[set[str]] = None,
) -> list[Path]:
    """
    Recursively discover files under root, excluding any files that live inside
    directories named in skip_dirnames.

    If include_dirnames is provided, only include files whose *top-level* folder
    (relative to root) is in include_dirnames. Root-level files (no parent dir)
    are always included.
    """
    files: list[Path] = []
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if _should_skip(p, root, skip_dirnames):
            continue

        rel = p.relative_to(root)

        # Root-level files are included by default
        if len(rel.parts) == 1:
            files.append(p.resolve())
            continue

        top = rel.parts[0]
        if include_dirnames is not None and top not in include_dirnames:
            continue

        files.append(p.resolve())

    return files
def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Filter crawled Canvas course files")

    p.add_argument(
        "--course-root",
        required=True,
        help="Path to course root (e.g. runs/<master>/canvas/output/<course_id>)",
    )
    p.add_argument(
        "--summary-json",
        required=True,
        help="Where to write a JSON summary of filtering decisions.",
    )

    # Exclusions
    p.add_argument(
        "--exclude-csv",
        default=None,
        help="CSV listing files to exclude (paths relative to course root).",
    )

    # NEW: title blacklist
    p.add_argument(
        "--title-blacklist",
        default=",".join(_DEFAULT_TITLE_BLACKLIST),
        help=(
            "Comma-separated list of case-insensitive substrings; "
            "if any appears in the JSON title/module_name or file name, "
            "the file is removed. "
            'Example: "midterm,Exam,Solution"'
        ),
    )

    # Token / size filter
    p.add_argument(
        "--min-token-count",
        type=int,
        default=None,
        help="Drop text-like files with fewer than this many tokens.",
    )

    # Dedupe
    dedupe_group = p.add_mutually_exclusive_group()
    dedupe_group.add_argument(
        "--dedupe",
        dest="dedupe",
        action="store_true",
        help="Enable duplicate removal (default).",
    )
    dedupe_group.add_argument(
        "--no-dedupe",
        dest="dedupe",
        action="store_false",
        help="Disable duplicate removal.",
    )
    p.set_defaults(dedupe=True)

    # Include / skip dirnames
    p.add_argument(
        "--include-dirnames",
        default=None,
        help="Comma-separated top-level folders to filter (others are ignored).",
    )
    p.add_argument(
        "--skip-dirnames",
        default=None,
        help="Comma-separated dirnames to always skip (in addition to locked,json_output).",
    )

    # Dry run
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Only log what would be deleted; do not actually remove files.",
    )

    # Logging removed files
    p.add_argument(
        "--log-removed",
        action="store_true",
        default=False,
        help="Print each file that is (or would be) removed, with the reason.",
    )
    p.add_argument(
        "--log-removed-to",
        default=None,
        help="Write removal events to this path as JSONL (one JSON object per line).",
    )
    p.add_argument(
        "--max-removed-in-summary",
        type=int,
        default=2000,
        help="Max removed file entries to embed in summary JSON (set 0 to disable).",
    )

    return p.parse_args(argv)


def _csv_to_set(s: Optional[str]) -> Optional[set[str]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",")]
    out = {p for p in parts if p}
    return out or None


def _log_event(
    *,
    event: Dict[str, Any],
    log_removed: bool,
    jsonl_fp,
) -> None:
    """
    event keys: action, reason, path, (optional) dupe_of, token_count, suffix
    """
    if log_removed:
        extra = ""
        if event.get("dupe_of"):
            extra = f" (dupe_of={event['dupe_of']})"
        if event.get("token_count") is not None:
            extra += f" (tokens={event['token_count']})"
        print(f"[filterer] {event['action']}: {event['reason']}: {event['path']}{extra}")

    if jsonl_fp is not None:
        jsonl_fp.write(json.dumps(event, ensure_ascii=False) + "\n")

def _load_metadata_index(course_root: Path) -> Dict[Path, Dict[str, Any]]:
    """
    Load a metadata index mapping resolved file paths to their JSON metadata.

    We scan ALL *.json files under <course_root>/json_output (recursively) and
    collect any dict that contains a "raw_file_path" field.

    This supports:
      - A single list file (e.g. items.json: [ {...}, {...}, ... ])
      - Per-item JSON files (each is a dict with raw_file_path)
      - Container dicts with an "items" list of objects
    """
    metadata_root = course_root / "json_output"
    if not metadata_root.exists():
        return {}

    index: Dict[Path, Dict[str, Any]] = {}

    for json_path in metadata_root.rglob("*.json"):
        try:
            raw = json_path.read_text(encoding="utf-8")
            data = json.loads(raw)
        except Exception:
            # Ignore malformed JSON and keep going
            continue

        # compute json path relative to course_root (for logging & deletion)
        try:
            rel_json_path = json_path.relative_to(course_root)
        except ValueError:
            rel_json_path = json_path

        # Case 1: top-level list of items
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                raw_file_path = item.get("raw_file_path")
                if not raw_file_path:
                    continue
                path = (course_root / raw_file_path).resolve()
                # annotate with the JSON file that defined this item
                item["_json_path"] = str(rel_json_path)
                index[path] = item

        # Case 2: top-level dict
        elif isinstance(data, dict):
            # 2a: single item dict with raw_file_path
            raw_file_path = data.get("raw_file_path")
            if raw_file_path:
                path = (course_root / raw_file_path).resolve()
                data["_json_path"] = str(rel_json_path)
                index[path] = data

            # 2b: container with an "items" list
            items = data.get("items")
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    raw_file_path = item.get("raw_file_path")
                    if not raw_file_path:
                        continue
                    path = (course_root / raw_file_path).resolve()
                    item["_json_path"] = str(rel_json_path)
                    index[path] = item

    return index
def run_filtering(args: argparse.Namespace) -> Dict[str, Any]:
    course_root = Path(args.course_root).resolve()
    summary_path = Path(args.summary_json).resolve()

    if not course_root.exists():
        raise FileNotFoundError(f"course_root does not exist: {course_root}")

    # Always skip these at minimum
    skip_dirnames = {"locked", "json_output"}
    extra_skip = _csv_to_set(getattr(args, "skip_dirnames", None))
    if extra_skip:
        skip_dirnames |= extra_skip

    include_dirnames = _csv_to_set(getattr(args, "include_dirnames", None))

    exclusion_set: set[Path] = set()
    if args.exclude_csv:
        exclusion_set = _read_exclusion_csv(course_root, Path(args.exclude_csv))
        print(f"[filterer] Loaded {len(exclusion_set)} exclusion paths from CSV")

    min_tokens: Optional[int] = args.min_token_count
    dedupe_enabled: bool = bool(args.dedupe)
    dry_run: bool = bool(args.dry_run)

    log_removed: bool = bool(getattr(args, "log_removed", False))
    max_removed_in_summary: int = int(getattr(args, "max_removed_in_summary", 2000))

    # parse title blacklist (normalized to lowercase)
    title_blacklist_raw = _csv_to_set(getattr(args, "title_blacklist", None))
    title_blacklist = {w.lower() for w in title_blacklist_raw} if title_blacklist_raw else set()

    # Compile whole-word/phrase regex patterns for each blacklist term
    title_blacklist_patterns: Dict[str, re.Pattern] = {
        term: re.compile(r"\b" + re.escape(term) + r"\b")
        for term in title_blacklist
    }

    jsonl_fp = None
    if getattr(args, "log_removed_to", None):
        log_path = Path(args.log_removed_to).resolve()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        jsonl_fp = log_path.open("w", encoding="utf-8")

    all_files = _discover_files_under(
        course_root,
        skip_dirnames=skip_dirnames,
        include_dirnames=include_dirnames,
    )

    # load metadata index
    metadata_index = _load_metadata_index(course_root)

    total_considered = 0
    removed_csv = 0
    removed_dupe = 0
    removed_small = 0
    removed_title_blacklist = 0

    # Track removals for summary (optional)
    removed_files: list[Dict[str, Any]] = []
    removed_examples: Dict[str, list[str]] = {
        "csv": [],
        "dupe": [],
        "small": [],
        "title_blacklist": [],
    }

    def maybe_record(event: Dict[str, Any]) -> None:
        reason = event.get("reason")
        if reason in removed_examples and len(removed_examples[reason]) < 25:
            removed_examples[reason].append(event["path"])
        if max_removed_in_summary > 0 and len(removed_files) < max_removed_in_summary:
            removed_files.append(event)

    seen_hashes: dict[str, Path] = {}

    try:
        for p in all_files:
            total_considered += 1

            action = "would_remove" if dry_run else "removed"
            rel_str = str(p.relative_to(course_root)) if p.is_relative_to(course_root) else str(p)

            # look up metadata & associated JSON file once
            meta = metadata_index.get(p.resolve())
            json_rel_str: Optional[str] = None
            if meta is not None:
                jp = meta.get("_json_path")
                if isinstance(jp, str):
                    json_rel_str = jp

            def delete_sidecar_json() -> None:
                if json_rel_str is None or dry_run:
                    return
                json_path = (course_root / json_rel_str).resolve()
                json_path.unlink(missing_ok=True)

            # 1) Explicit CSV exclusion
            if p in exclusion_set:
                removed_csv += 1
                event: Dict[str, Any] = {
                    "action": action,
                    "reason": "csv",
                    "path": rel_str,
                }
                if json_rel_str is not None:
                    event["json_path"] = json_rel_str

                # New: annotate JSON in dry-run
                if dry_run and json_rel_str is not None and meta is not None:
                    _annotate_json_removal(
                        course_root=course_root,
                        json_rel_str=json_rel_str,
                        meta=meta,
                        event=event,
                    )

                _log_event(event=event, log_removed=log_removed, jsonl_fp=jsonl_fp)
                maybe_record(event)
                if not dry_run:
                    p.unlink(missing_ok=True)
                    delete_sidecar_json()
                continue


               # 2) title / metadata blacklist (whole-word/phrase, TITLE ONLY)
            if title_blacklist_patterns:
                # meta & json_rel_str are computed earlier in the loop:
                # meta = metadata_index.get(p.resolve())
                # json_rel_str = meta.get("_json_path") if meta else None

                title_text: Optional[str] = None
                if meta:
                    val = meta.get("title")
                    if isinstance(val, str):
                        title_text = val.lower()

                bad_term = None
                if title_text:
                    for term, pattern in title_blacklist_patterns.items():
                        if pattern.search(title_text):
                            bad_term = term
                            break

                if bad_term is not None:
                    removed_title_blacklist += 1
                    event: Dict[str, Any] = {
                        "action": action,
                        "reason": "title_blacklist",
                        "path": rel_str,
                        "blacklisted_term": bad_term,
                    }
                    if json_rel_str is not None:
                        event["json_path"] = json_rel_str

                    if dry_run and json_rel_str is not None and meta is not None:
                        _annotate_json_removal(
                            course_root=course_root,
                            json_rel_str=json_rel_str,
                            meta=meta,
                            event=event,
                        )

                    _log_event(event=event, log_removed=log_removed, jsonl_fp=jsonl_fp)
                    maybe_record(event)
                    if not dry_run:
                        p.unlink(missing_ok=True)
                        delete_sidecar_json()
                    continue



            # 3) Duplicate removal
            if dedupe_enabled:
                try:
                    data = p.read_bytes()
                except OSError:
                    data = None

                if data is not None:
                    h = hashlib.sha256(data).hexdigest()
                    if h in seen_hashes:
                        removed_dupe += 1
                        dupe_of = seen_hashes[h]
                        dupe_of_rel = (
                            str(dupe_of.relative_to(course_root))
                            if dupe_of.is_relative_to(course_root)
                            else str(dupe_of)
                        )
                        event = {
                            "action": action,
                            "reason": "dupe",
                            "path": rel_str,
                            "dupe_of": dupe_of_rel,
                        }
                        if json_rel_str is not None:
                            event["json_path"] = json_rel_str

                        if dry_run and json_rel_str is not None and meta is not None:
                            _annotate_json_removal(
                                course_root=course_root,
                                json_rel_str=json_rel_str,
                                meta=meta,
                                event=event,
                            )
                        _log_event(event=event, log_removed=log_removed, jsonl_fp=jsonl_fp)
                        maybe_record(event)
                        if not dry_run:
                            p.unlink(missing_ok=True)
                            delete_sidecar_json()
                        continue
                    else:
                        seen_hashes[h] = p

            # 4) Low-token removal (only text-like files)
            if min_tokens is not None and p.suffix.lower() in _TEXT_SUFFIXES:
                try:
                    text = p.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    text = ""
                tok_count = _estimate_token_count(text)
                if tok_count < min_tokens:
                    removed_small += 1
                    event = {
                        "action": action,
                        "reason": "small",
                        "path": rel_str,
                        "token_count": tok_count,
                        "suffix": p.suffix.lower(),
                    }
                    if json_rel_str is not None:
                        event["json_path"] = json_rel_str

                    if dry_run and json_rel_str is not None and meta is not None:
                        _annotate_json_removal(
                            course_root=course_root,
                            json_rel_str=json_rel_str,
                            meta=meta,
                            event=event,
                        )

                    _log_event(event=event, log_removed=log_removed, jsonl_fp=jsonl_fp)
                    maybe_record(event)
                    if not dry_run:
                        p.unlink(missing_ok=True)
                        delete_sidecar_json()
                    continue
    finally:
        if jsonl_fp is not None:
            jsonl_fp.close()

    summary: Dict[str, Any] = {
        "course_root": str(course_root),
        "total_considered": total_considered,
        "removed_csv": removed_csv,
        "removed_dupe": removed_dupe,
        "removed_small": removed_small,
        "removed_title_blacklist": removed_title_blacklist,
        "dry_run": dry_run,
        "min_token_count": min_tokens,
        "dedupe": dedupe_enabled,
        "skip_dirnames": sorted(skip_dirnames),
        "include_dirnames": sorted(include_dirnames) if include_dirnames else None,
        "exclude_csv": str(Path(args.exclude_csv).resolve()) if args.exclude_csv else None,
        "title_blacklist": sorted(title_blacklist) if title_blacklist else None,
        "log_removed": log_removed,
        "log_removed_to": str(Path(args.log_removed_to).resolve()) if getattr(args, "log_removed_to", None) else None,
        "removed_examples": removed_examples,
        "removed_files_capped": (max_removed_in_summary > 0),
        "removed_files_cap": max_removed_in_summary,
        "removed_files": removed_files if max_removed_in_summary > 0 else None,
    }

    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(
        f"[filterer] Summary: considered={total_considered}, "
        f"csv={removed_csv}, title_blacklist={removed_title_blacklist}, "
        f"dupes={removed_dupe}, small={removed_small}, dry_run={dry_run}"
    )

    return summary



def main(argv: Optional[list[str]] = None) -> int:
    # Parse CLI args if none were passed programmatically
    args = parse_args(argv)
    run_filtering(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())