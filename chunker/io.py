from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple


def iter_json_records(json_output_dir: Path) -> Iterable[Tuple[Path, Dict[str, Any]]]:
    for jf in sorted(json_output_dir.glob("*.json")):
        if jf.is_file():
            yield jf, json.loads(jf.read_text(encoding="utf-8"))


def resolve_md_path(master_run_dir: Path, md_rel: str) -> Path:
    return (master_run_dir / md_rel).resolve()


def rel_to(path: Path, start: Path) -> str:
    try:
        return str(path.resolve().relative_to(start.resolve()))
    except Exception:
        return str(path.resolve())
