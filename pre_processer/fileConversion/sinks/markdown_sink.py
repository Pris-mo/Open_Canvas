from __future__ import annotations

import re
import logging
from dataclasses import dataclass
from pathlib import Path

from ..schema import ConversionResult


_SAFE_CHARS = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass
class MarkdownSink:
    out_dir: Path
    logger: logging.Logger
    source_root: Path | None = None  # NEW: used to mirror directory structure

    def __post_init__(self):
        self.out_dir.mkdir(parents=True, exist_ok=True)

    def _safe_part(self, part: str) -> str:
        # keep it stable + filesystem-friendly
        part = part.replace(" ", "_")
        part = _SAFE_CHARS.sub("_", part)
        part = part.strip("._") or "untitled"
        return part

    def _relative_source_path(self, source_path: str) -> Path:
        p = Path(source_path)

        # If a root is provided and source is under it, mirror relative path from root.
        if self.source_root is not None:
            try:
                return p.relative_to(self.source_root)
            except Exception:
                pass  # fall back

        # If it's absolute (but we don't know root), just use filename to avoid huge paths.
        if p.is_absolute():
            return Path(p.name)

        return p

    def _sanitize_relpath(self, rel: Path) -> Path:
        # sanitize each component, keep subdirs
        parts = [self._safe_part(x) for x in rel.parts]
        return Path(*parts)

    def write(self, result: ConversionResult) -> Path | None:
        if not result.markdown.strip():
            return None

        rel = self._relative_source_path(result.source_path)
        rel = self._sanitize_relpath(rel)

        # Replace original extension with .md
        rel_md = rel.with_suffix(".md")

        out_path = self.out_dir / rel_md
        out_path.parent.mkdir(parents=True, exist_ok=True)

        out_path.write_text(result.markdown, encoding="utf-8")
        self.logger.debug("source_root=%s", self.source_root)
        self.logger.debug("source_path=%s", result.source_path)
        self.logger.debug("Wrote markdown → %s", out_path)
        return out_path
