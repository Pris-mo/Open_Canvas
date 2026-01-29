from __future__ import annotations

import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Optional

from markitdown import MarkItDown

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore
    
from ..schema import Engine, ConversionMode, ConversionResult, Outcome



class MarkItDownConverter:
    """
    Thin wrapper around MarkItDown that normalizes output into ConversionResult.
    """

    def __init__(
        self,
        *,
        llm_api_key: Optional[str],
        llm_model: str,
        enable_llm: bool,
        logger,
    ):
        self.logger = logger
        self.llm_model = llm_model
        self.enable_llm = enable_llm

        self._llm_api_key = llm_api_key
        self._llm_client = None

        # Lazy-initialized MarkItDown instances
        self._md_lean: Optional[MarkItDown] = None
        self._md_llm: Optional[MarkItDown] = None

    def _get_lean(self) -> MarkItDown:
        if self._md_lean is None:
            self.logger.debug("Init MarkItDown(lean): enable_plugins=False")
            self._md_lean = MarkItDown(enable_plugins=False)
        return self._md_lean

    def _get_llm(self) -> MarkItDown:
        if not self.enable_llm:
            raise RuntimeError("LLM conversion requested but enable_llm=False")

        if not self._llm_api_key:
            raise RuntimeError("OPENAI_API_KEY is not set; cannot run LLM conversion")

        if OpenAI is None:
            raise RuntimeError("openai package not available; cannot run LLM conversion")

        if self._llm_client is None:
            self.logger.debug("Init OpenAI client for MarkItDown")
            self._llm_client = OpenAI(api_key=self._llm_api_key)

        if self._md_llm is None:
            self.logger.debug("Init MarkItDown(llm): model=%s", self.llm_model)
            self._md_llm = MarkItDown(llm_client=self._llm_client, llm_model=self.llm_model)
        return self._md_llm

    def convert(self, path: str, mode: ConversionMode, *, attempt: int = 1) -> ConversionResult:
        p = Path(path)
        start = time.perf_counter()

        try:
            md = self._get_llm() if mode == ConversionMode.LLM else self._get_lean()
            raw = md.convert(str(p))

            markdown = (getattr(raw, "text_content", "") or "")
            outcome = Outcome.OK if markdown.strip() else Outcome.BLANK

            meta: Dict[str, Any] = {
                "source_name": p.name,
                "suffix": p.suffix.lower(),
            }

            # Best-effort capture of other fields (future-proofing)
            # We keep it very conservative to avoid fragile assumptions.
            for attr in ("metadata", "content", "tables", "images", "links", "warnings"):
                if hasattr(raw, attr):
                    try:
                        meta[f"raw_{attr}"] = getattr(raw, attr)
                    except Exception:
                        pass

            dur_ms = int((time.perf_counter() - start) * 1000)

            return ConversionResult(
                source_path=str(p),
                mode_used=mode,                
                engine_used=Engine.MARKITDOWN,
                outcome=outcome,
                markdown=markdown,
                meta=meta,
                duration_ms=dur_ms,
                attempt=attempt,
            )

        except Exception as e:
            dur_ms = int((time.perf_counter() - start) * 1000)
            self.logger.debug("Converter exception | mode=%s | path=%s | err=%s", mode, path, e)

            return ConversionResult(
                source_path=str(p),
                mode_used=mode,                
                engine_used=Engine.MARKITDOWN,
                outcome=Outcome.FAILED,
                markdown="",
                error=str(e),
                meta={"source_name": p.name, "suffix": p.suffix.lower()},
                duration_ms=dur_ms,
                attempt=attempt,
            )
