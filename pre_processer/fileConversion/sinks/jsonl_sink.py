from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from ..schema import ConversionResult
import logging


@dataclass
class JsonlSink:
    out_path: Path
    logger: logging.Logger

    def __post_init__(self):
        self.out_path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, result: ConversionResult) -> None:
        record = asdict(result)
        # Keep JSONL robust if meta/artifacts contain non-serializable objects
        line = json.dumps(record, ensure_ascii=False, default=str)
        with self.out_path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
        self.logger.debug("Appended JSONL → %s", self.out_path)
