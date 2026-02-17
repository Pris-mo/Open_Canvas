from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

# Adjust import path to your orchestrator module filename
# If your orchestrator lives at orchestrator/run_pipeline.py:
from orchestrator.run_pipeline import run_pipeline, _load_yaml


_ALWAYS_SKIP = {"locked", "json_output"}


def _read_env_file(path: Path) -> dict[str, str]:
    """
    Minimal .env reader:
    - Supports KEY=VALUE
    - Ignores blank lines and comments starting with #
    - Strips surrounding single/double quotes on values
    """
    if not path.exists():
        raise FileNotFoundError(f".env file not found: {path}")

    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip()
        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
            v = v[1:-1]
        out[k] = v
    return out


def _parse_course_url(course_url: str) -> tuple[str, int]:
    """
    Returns (base_url, course_id).
    Requires path containing /courses/<digits>.
    Accepts deep links: /courses/<id>/pages/..., /modules, etc.
    """
    u = urlparse(course_url)
    if not u.scheme or not u.netloc:
        raise ValueError(f"Invalid URL: {course_url}")

    # Find /courses/<id> anywhere in the path
    m = re.search(r"/courses/(\d+)(?:/|$)", u.path)
    if not m:
        raise ValueError(
            "Course URL must include '/courses/<id>'. Example: https://learn.canvas.net/courses/3376"
        )

    course_id = int(m.group(1))
    base_url = f"{u.scheme}://{u.netloc}"
    return base_url, course_id


def _csv_set(s: Optional[str]) -> Optional[set[str]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",")]
    out = {p for p in parts if p}
    return out or None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Canvas -> Markdown -> Chunk orchestrator")

    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--config", help="YAML config path (YAML mode).")
    mode.add_argument("--course-url", help="Canvas course URL (CLI mode).")

    # CLI-mode auth
    p.add_argument("--canvas-token", default=None, help="Canvas API token (CLI mode).")
    p.add_argument("--env-file", default=None, help="Path to .env file (CLI mode).")

    # URL/base overrides (CLI mode)
    p.add_argument(
        "--api-base-url",
        default=None,
        help="Optional override for Canvas API base URL (rare UI/API host mismatch).",
    )

    # Optional: OpenAI
    p.add_argument("--openai-api-key", default=None, help="OpenAI API key (CLI mode).")

    # Behavior knobs
    p.add_argument("--include", default=None, help="Comma-separated top-level folders to convert/chunk.")
    p.add_argument("--include-frontmatter", action="store_true", help="Prefix each chunk with metadata frontmatter.")
    p.add_argument("--depth-limit", type=int, default=10, help="Canvas crawl depth limit (CLI mode).")
    p.add_argument("--model", default="gpt-4o", help="LLM model for conversion fallback.")
    p.add_argument("--runs-root", default="runs", help="Runs root directory (relative to repo root).")
    p.add_argument("--run-name", default=None, help="Optional run name; default is timestamp.")

    # Verbose flags
    p.add_argument("--crawler-verbose", action="store_true")
    p.add_argument("--conversion-verbose", action="store_true")

    # Scripts (allow override without YAML)
    p.add_argument(
        "--crawler-module",
        default="canvas_crawler.cli",  # or whatever your real entrypoint is
        help="Crawler entrypoint module for python -m (e.g. 'canvas_crawler.cli').",
    )
    p.add_argument(
        "--conversion-script",
        default="pre_processer.run_conversion",   # 👈 module path, not file path
        help="Conversion entrypoint (module path or script path).",
    )

    # Chunking flags
    p.add_argument("--chunking", dest="chunking_enabled", action="store_true", default=True)
    p.add_argument("--no-chunking", dest="chunking_enabled", action="store_false")
    p.add_argument(
        "--write-chunk-files",
        dest="write_chunk_files",
        action="store_true",
        default=True,
        help="Write chunk files to disk (default: enabled).",
    )
    p.add_argument(
        "--no-write-chunk-files",
        dest="write_chunk_files",
        action="store_false",
        help="Disable writing chunk files to disk.",
    )

    # Filtering flags (pre-conversion)
    p.add_argument(
        "--filter",
        dest="filter_enabled",
        action="store_true",
        default=False,
        help="Enable pre-conversion filtering of crawled files.",
    )
    p.add_argument(
        "--no-filter",
        dest="filter_enabled",
        action="store_false",
        help="Disable pre-conversion filtering (default).",
    )
    p.add_argument(
        "--filter-script",
        default="filterer.cli",
        help="Filtering entrypoint (module path or script path), e.g. 'filterer.cli'.",
    )
    p.add_argument(
        "--filter-min-tokens",
        type=int,
        default=None,
        help="Drop text-like files with fewer than this many tokens during filtering.",
    )
    p.add_argument(
        "--filter-exclude-csv",
        default=None,
        help="CSV listing files to exclude during filtering (paths relative to course root).",
    )
    p.add_argument(
        "--filter-title-blacklist",
        default=None,
        help=(
            "Comma-separated list of case-insensitive substrings; "
            "if any appears in the JSON title/module_name or file name, "
            "the file is removed. Overrides the filterer's default blacklist."
        ),
    )
    p.add_argument(
        "--filter-dry-run",
        action="store_true",
        help="Run filtering in dry-run mode (log but do not delete files).",
    )
    p.add_argument(
        "--filter-log-removed",
        action="store_true",
        help="Print each file that is (or would be) removed during filtering.",
    )
    p.add_argument(
        "--filter-log-removed-to",
        default=None,
        help="Write filtering removal events to this path as JSONL (one JSON object per line).",
    )
    p.add_argument(
        "--no-filter-dedupe",
        dest="filter_dedupe",
        action="store_false",
        default=True,
        help="Disable duplicate removal during filtering.",
    )

    return p.parse_args()


def build_cfg_from_cli(args: argparse.Namespace, repo_root: Path) -> dict[str, Any]:
    base_url, course_id = _parse_course_url(args.course_url)

    # Strict: no environment-variable fallback in CLI mode.
    env_data: dict[str, str] = {}
    if args.env_file:
        env_path = Path(args.env_file).expanduser()
        if not env_path.is_absolute():
            env_path = (repo_root / env_path).resolve()
        env_data = _read_env_file(env_path)

    canvas_token = args.canvas_token or env_data.get("CANVAS_TOKEN")
    if not canvas_token:
        raise ValueError(
            "Canvas token required in CLI mode. Provide --canvas-token OR --env-file containing CANVAS_TOKEN."
        )

    openai_key = args.openai_api_key or env_data.get("OPENAI_API_KEY")
    enable_llm = bool(openai_key)

    # In CLI mode, canvas_url is the API base URL
    canvas_api_url = args.api_base_url or base_url

    include_set = _csv_set(args.include)
    if include_set is not None:
        include_set -= _ALWAYS_SKIP  # never allow these
        if not include_set:
            raise ValueError("After removing locked/json_output, --include is empty. Provide at least one folder.")

    cfg: dict[str, Any] = {
        "run": {
            "runs_root": args.runs_root,
            "name": args.run_name,
        },
        "canvas": {
            "crawler_module": args.crawler_module,
            "canvas_url": canvas_api_url,
            "course_id": course_id,
            "depth_limit": args.depth_limit,
            "verbose": bool(args.crawler_verbose),
            "token": canvas_token,
        },
        "conversion": {
            "script": args.conversion_script,
            "model": args.model,
            "enable_llm": enable_llm,
            "verbose": bool(args.conversion_verbose),
            "source_root_mode": "course_root",
            # always skip these (enforced in orchestrator too)
            "skip_dirnames": sorted(_ALWAYS_SKIP),
            # optional include filter (top-level folders)
            "include_dirnames": sorted(include_set) if include_set is not None else None,
        },
        "bridge": {
            "json_output_dirname": "json_output",
            "raw_key": "raw_file_path",
            "legacy_key": "file_path",
            "md_key": "md_file_path",
            "md_value_mode": "relative_to_master_run",
            "atomic_write": True,
        },
        "chunking": {
            "enabled": bool(args.chunking_enabled),
            "write_chunk_files": bool(args.write_chunk_files),
            # pass through; your chunker can read PIPELINE_CONFIG or a new env var / CLI arg
            "include_frontmatter": bool(args.include_frontmatter),
        },
        "filtering": {
            "enabled": bool(args.filter_enabled),
            "script": args.filter_script,
            "exclude_csv": args.filter_exclude_csv,
            "min_token_count": args.filter_min_tokens,
            "dedupe": bool(getattr(args, "filter_dedupe", True)),
            "dry_run": bool(args.filter_dry_run),
            "log_removed": bool(args.filter_log_removed),
            "log_removed_to": args.filter_log_removed_to,
            # Pass the same include list to filtering by default
            "include_dirnames": sorted(include_set) if include_set is not None else None,
            # title_blacklist: only set if user supplied it; otherwise filterer uses its default
            **(
                {"title_blacklist": args.filter_title_blacklist}
                if args.filter_title_blacklist is not None
                else {}
            ),
        },
        "upload": {"enabled": False},
        # stash tokens for your own future use if desired
        "_secrets": {
            "canvas_token_present": True,
            "openai_key_present": bool(openai_key),
        },
    }

    # Inject secrets into env for subprocesses (crawler/conversion/chunker).
    # NOTE: Your orchestrator currently uses os.environ.copy() inside each stage,
    # so setting these in os.environ here is the easiest way to pass them.
    import os
    os.environ["CANVAS_TOKEN"] = canvas_token
    if openai_key:
        os.environ["OPENAI_API_KEY"] = openai_key

    # Optional: let chunker know frontmatter behavior if you want an env flag today
    if args.include_frontmatter:
        os.environ["PIPELINE_INCLUDE_FRONTMATTER"] = "1"
    else:
        os.environ.pop("PIPELINE_INCLUDE_FRONTMATTER", None)

    return cfg


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    # YAML mode
    if args.config:
        cfg_path = Path(args.config).expanduser()
        if not cfg_path.is_absolute():
            cfg_path = (repo_root / cfg_path).resolve()
        cfg = _load_yaml(cfg_path)
        return run_pipeline(cfg, repo_root, cfg_path)

    # CLI mode (no YAML consideration)
    cfg = build_cfg_from_cli(args, repo_root)
    return run_pipeline(cfg, repo_root, cfg_path=None)


if __name__ == "__main__":
    raise SystemExit(main())
