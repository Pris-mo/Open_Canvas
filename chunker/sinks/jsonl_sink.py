from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class JsonlSink:
    out_path: Path

    def __post_init__(self) -> None:
        self.out_path.parent.mkdir(parents=True, exist_ok=True)

    def write_line(self, obj: Dict[str, Any]) -> None:
        with self.out_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
