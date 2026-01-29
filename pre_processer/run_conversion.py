from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

from dotenv import load_dotenv

from fileConversion.config import AppConfig
from fileConversion.pipeline import Pipeline


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Convert files to Markdown using MarkItDown with optional LLM fallback.")
    p.add_argument("--verbose", action="store_true", help="Enable debug logging.")
    p.add_argument("--no-llm", action="store_true", help="Disable LLM fallback entirely.")
    p.add_argument("--model", default="gpt-4o", help="LLM model name (when LLM is enabled).")
    p.add_argument("--env", default=None, help="Path to .env file (optional).")
    p.add_argument("--runs-root", default="runs", help="Directory for run outputs.")
    p.add_argument("paths", nargs="*", help="Files to convert.")
    return p.parse_args()

def expand_paths(paths: List[str]) -> List[str]:
    expanded: List[str] = []

    for p in paths:
        path = Path(p)
        if path.is_dir():
            for child in path.iterdir():
                if child.is_file():
                    expanded.append(str(child))
        else:
            expanded.append(str(path))

    return expanded


def main() -> int:
    args = parse_args()

    # Load env (optional)
    if args.env:
        load_dotenv(args.env)
    else:
        default_env = Path(__file__).parent / ".env"
        if default_env.exists():
            load_dotenv(default_env)

    cfg = AppConfig(
        runs_root=Path(args.runs_root),
        enable_llm_fallback=(not args.no_llm),
        llm_model=args.model,
        verbose=args.verbose,
    )

    if not args.paths:
        print("No input paths provided.")
        return 2

    pipeline = Pipeline.from_config(cfg)
    expanded_paths = expand_paths(args.paths)
    if not expanded_paths:
        print("No files found in provided paths.")
        return 2
    
    print(f"Discovered {len(expanded_paths)} file(s) to process.")
    summary = pipeline.run(expanded_paths)
    print(f"Summary: {summary.to_dict()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
