from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any, Dict

# import your orchestrator module; adjust import to match your file name
# If your orchestrator file is orchestrator/run_pipeline.py, then:
from orchestrator.run_pipeline import run_pipeline, _load_yaml


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the Canvas -> Markdown -> Chunk pipeline (CLI).")

    # Optional defaults file (lets you keep YAML as fallback)
    p.add_argument("--config", default=None, help="Optional YAML config for defaults.")

    # Run settings
    p.add_argument("--runs-root", default="runs", help="Runs root directory (relative to repo root).")
    p.add_argument("--run-name", default=None, help="Run name; default is timestamp.")

    # Canvas crawler
    p.add_argument("--course-id", required=True, help="Canvas course ID.")
    p.add_argument("--canvas-url", required=True, help="Canvas base URL (e.g. https://learn.canvas.net).")
    p.add_argument("--crawler-script", default="canvas_crawler/canvas_crawler.py")
    p.add_argument("--depth-limit", type=int, default=15)
    p.add_argument("--crawler-verbose", action="store_true")

    # Conversion
    p.add_argument("--conversion-script", default="pre_processer/run_conversion.py")
    p.add_argument("--model", default="gpt-4o")
    p.add_argument("--enable-llm", action="store_true", default=False)
    p.add_argument("--no-llm", action="store_true", default=False)
    p.add_argument("--conversion-verbose", action="store_true")
    p.add_argument("--skip-dirname", action="append", default=[], help="Repeatable. Example: --skip-dirname locked")

    # Bridge
    p.add_argument("--json-output-dirname", default="json_output")
    p.add_argument("--raw-key", default="raw_file_path")
    p.add_argument("--legacy-key", default="file_path")
    p.add_argument("--md-key", default="md_file_path")
    p.add_argument(
        "--md-value-mode",
        choices=["relative_to_master_run", "relative_to_repo", "absolute"],
        default="relative_to_master_run",
    )
    p.add_argument("--atomic-write", action="store_true", default=True)
    p.add_argument("--no-atomic-write", action="store_true", default=False)

    # Chunking
    p.add_argument("--chunking-enabled", action="store_true", default=False)
    p.add_argument("--write-chunk-files", action="store_true", default=False)

    return p.parse_args()


def deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Shallow+recursive merge for dicts. override wins.
    """
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def build_cfg_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    # LLM flag logic:
    # - If user explicitly sets --no-llm, disable
    # - Else if user sets --enable-llm, enable
    # - Else leave default False (unless overridden by YAML defaults)
    enable_llm = False
    if args.no_llm:
        enable_llm = False
    elif args.enable_llm:
        enable_llm = True

    atomic_write = True
    if args.no_atomic_write:
        atomic_write = False
    elif args.atomic_write:
        atomic_write = True

    cfg: Dict[str, Any] = {
        "run": {
            "runs_root": args.runs_root,
            "name": args.run_name,
        },
        "canvas": {
            "crawler_script": args.crawler_script,
            "canvas_url": args.canvas_url,
            "course_id": int(args.course_id) if str(args.course_id).isdigit() else args.course_id,
            "depth_limit": args.depth_limit,
            "verbose": bool(args.crawler_verbose),
        },
        "conversion": {
            "script": args.conversion_script,
            "model": args.model,
            "enable_llm": enable_llm,
            "verbose": bool(args.conversion_verbose),
            "source_root_mode": "course_root",
            "skip_dirnames": args.skip_dirname or [],
        },
        "bridge": {
            "json_output_dirname": args.json_output_dirname,
            "raw_key": args.raw_key,
            "legacy_key": args.legacy_key,
            "md_key": args.md_key,
            "md_value_mode": args.md_value_mode,
            "atomic_write": atomic_write,
        },
        "chunking": {
            "enabled": bool(args.chunking_enabled),
            "write_chunk_files": bool(args.write_chunk_files),
        },
        "upload": {"enabled": False},
    }
    return cfg


def main() -> int:
    args = parse_args()

    repo_root = Path(__file__).resolve().parents[1]

    # Optional YAML defaults
    cfg_path = None
    base_cfg: Dict[str, Any] = {}
    if args.config:
        cfg_path = (Path(args.config).expanduser().resolve())
        base_cfg = _load_yaml(cfg_path)

    cli_cfg = build_cfg_from_args(args)
    cfg = deep_merge(base_cfg, cli_cfg) if base_cfg else cli_cfg

    # If user didn’t specify either enable/disable, and YAML provided enable_llm, keep it.
    # (Our build_cfg sets enable_llm False by default, so this merge matters.)

    # Also: let downstream tools know where config came from if you still want that behavior
    if cfg_path:
        os.environ["PIPELINE_CONFIG"] = str(cfg_path)

    return run_pipeline(cfg, repo_root, cfg_path)


if __name__ == "__main__":
    raise SystemExit(main())
