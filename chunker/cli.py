from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any, Dict, Optional

from .config import load_config, parse_chunking_config
from .pipeline import run_chunking


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Chunk markdown documents referenced by Canvas JSON records.")
    p.add_argument("--config", default=None, help="Path to pipeline config.yml (optional).")

    p.add_argument("--master-run", required=True, help="Path to master run dir (runs/<ts>).")
    p.add_argument("--json-output", required=True, help="Path to json_output dir for the course run.")
    p.add_argument("--course-id", type=int, default=None, help="Course ID (optional; used in source).")

    # Overrides
    p.add_argument("--md-key", default=None, help="Override md key (default from config: md_file_path).")
    p.add_argument("--chunk-size", type=int, default=None)
    p.add_argument("--chunk-overlap", type=int, default=None)
    p.add_argument("--write-chunk-files", action="store_true", help="Write each chunk to an individual .md file.")

    # Frontmatter toggles
    fm = p.add_mutually_exclusive_group()
    fm.add_argument("--frontmatter", action="store_true", help="Force enable YAML frontmatter in chunk text.")
    fm.add_argument("--no-frontmatter", action="store_true", help="Disable YAML frontmatter in chunk text.")

    # Output toggles
    md = p.add_mutually_exclusive_group()
    md.add_argument("--include-separate-metadata", action="store_true", help="Include metadata field in JSONL output.")
    md.add_argument("--no-separate-metadata", action="store_true", help="Omit metadata field in JSONL output.")

    src = p.add_mutually_exclusive_group()
    src.add_argument("--include-source", action="store_true", help="Include source field in JSONL output.")
    src.add_argument("--no-source", action="store_true", help="Omit source field in JSONL output.")

    return p.parse_args()


def main() -> int:
    args = parse_args()

    master_run_dir = Path(args.master_run).resolve()
    json_output_dir = Path(args.json_output).resolve()

    # Load config (optional)
    cfg: Dict[str, Any] = {}
    if args.config:
        cfg = load_config(Path(args.config).resolve())
    else:
        # Optional: PIPELINE_CONFIG env var
        env_cfg = os.environ.get("PIPELINE_CONFIG")
        if env_cfg and Path(env_cfg).exists():
            cfg = load_config(Path(env_cfg).resolve())

    chunk_cfg = parse_chunking_config(cfg)

    # Apply overrides
    if args.md_key:
        chunk_cfg = chunk_cfg.__class__(**{**chunk_cfg.__dict__, "md_key": args.md_key})

    if args.chunk_size is not None:
        chunk_cfg = chunk_cfg.__class__(**{**chunk_cfg.__dict__, "chunk_size": int(args.chunk_size)})

    if args.chunk_overlap is not None:
        chunk_cfg = chunk_cfg.__class__(**{**chunk_cfg.__dict__, "chunk_overlap": int(args.chunk_overlap)})

    if args.write_chunk_files:
        out = chunk_cfg.output.__class__(**{**chunk_cfg.output.__dict__, "write_individual_files": True})
        chunk_cfg = chunk_cfg.__class__(**{**chunk_cfg.__dict__, "output": out})

    # Frontmatter overrides
    if args.frontmatter:
        fm = chunk_cfg.frontmatter.__class__(**{**chunk_cfg.frontmatter.__dict__, "enabled": True})
        chunk_cfg = chunk_cfg.__class__(**{**chunk_cfg.__dict__, "frontmatter": fm})
    if args.no_frontmatter:
        fm = chunk_cfg.frontmatter.__class__(**{**chunk_cfg.frontmatter.__dict__, "enabled": False})
        chunk_cfg = chunk_cfg.__class__(**{**chunk_cfg.__dict__, "frontmatter": fm})

    # Output overrides
    if args.include_separate_metadata:
        out = chunk_cfg.output.__class__(**{**chunk_cfg.output.__dict__, "include_separate_metadata": True})
        chunk_cfg = chunk_cfg.__class__(**{**chunk_cfg.__dict__, "output": out})
    if args.no_separate_metadata:
        out = chunk_cfg.output.__class__(**{**chunk_cfg.output.__dict__, "include_separate_metadata": False})
        chunk_cfg = chunk_cfg.__class__(**{**chunk_cfg.__dict__, "output": out})

    if args.include_source:
        out = chunk_cfg.output.__class__(**{**chunk_cfg.output.__dict__, "include_source": True})
        chunk_cfg = chunk_cfg.__class__(**{**chunk_cfg.__dict__, "output": out})
    if args.no_source:
        out = chunk_cfg.output.__class__(**{**chunk_cfg.output.__dict__, "include_source": False})
        chunk_cfg = chunk_cfg.__class__(**{**chunk_cfg.__dict__, "output": out})

    out_path, summary = run_chunking(
        master_run_dir=master_run_dir,
        json_output_dir=json_output_dir,
        course_id=args.course_id,
        cfg=chunk_cfg,
    )

    print(f"Chunks written → {out_path}")
    print(f"Total chunks: {summary.total_chunks_written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
