from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import logging


from docling.document_converter import DocumentConverter  # main entry point :contentReference[oaicite:3]{index=3}

from ..schema import ConversionMode, ConversionResult, Outcome


@dataclass
class DoclingConverter:
    logger: logging.Logger
    allowed_formats: Optional[list[Any]] = None  # keep generic; Docling has InputFormat enum
    _converter: Optional[DocumentConverter] = None

    def _get_converter(self) -> DocumentConverter:
        if self._converter is None:
            # Keep it simple for now: default converter auto-detects formats. :contentReference[oaicite:4]{index=4}
            self._converter = DocumentConverter(allowed_formats=self.allowed_formats)
        return self._converter

    def convert(self, path: str, mode: ConversionMode, *, attempt: int = 1) -> ConversionResult:
        """
        Docling doesn't need 'lean vs llm' the way MarkItDown did.
        We accept `mode` only to match your pipeline interface; we record it but don't branch on it.
        """
        start = time.perf_counter()
        p = Path(path)

        try:
            conv = self._get_converter()
            res = conv.convert(str(p))  # returns a conversion result with `.document` :contentReference[oaicite:5]{index=5}
            doc = res.document

            # Export to Markdown (supported output format). :contentReference[oaicite:6]{index=6}
            # Docling’s API exposes export methods; exact method name can vary by version.
            # Common pattern: doc.export_to_markdown()
            markdown = doc.export_to_markdown()

            outcome = Outcome.OK if markdown and markdown.strip() else Outcome.BLANK

            duration_ms = int((time.perf_counter() - start) * 1000)

            return ConversionResult(
                source_path=str(p),
                mode_used=mode,  # keep as-is for now
                outcome=outcome,
                markdown=markdown or "",
                error=None,
                warnings=[],
                meta={
                    "source_name": p.name,
                    "suffix": p.suffix.lower(),
                    "engine": "docling",
                },
                artifacts={},
                duration_ms=duration_ms,
                attempt=attempt,
            )

        except Exception as e:
            duration_ms = int((time.perf_counter() - start) * 1000)
            return ConversionResult(
                source_path=str(p),
                mode_used=mode,
                outcome=Outcome.FAILED,
                markdown="",
                error=str(e),
                warnings=[],
                meta={
                    "source_name": p.name,
                    "suffix": p.suffix.lower(),
                    "engine": "docling",
                },
                artifacts={},
                duration_ms=duration_ms,
                attempt=attempt,
            )
