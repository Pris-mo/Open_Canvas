from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..schema import ConversionResult
import logging


@dataclass
class MarkdownSink:
    out_dir: Path
    logger: logging.Logger

    def __post_init__(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def _safe_stem(self, source_path: str) -> str:
        # simple, stable filename: keep original name, replace spaces
        p = Path(source_path)
        stem = p.stem.replace(" ", "_")
        return stem

    def write(self, result: ConversionResult) -> Path | None:
        if not result.markdown.strip():
            return None

        stem = self._safe_stem(result.source_path)
        out_path = self.out_dir / f"{stem}.md"
        out_path.write_text(result.markdown, encoding="utf-8")

        self.logger.debug("Wrote markdown → %s", out_path)
        return out_path
