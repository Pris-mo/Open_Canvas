from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

from .schema import ConversionMode, ConversionResult, Outcome


@dataclass(frozen=True)
class FallbackPolicy:
    """
    Encodes which modes to try and when to retry.
    Keep this logic centralized so it doesn't leak into pipeline/sinks.
    """
    enable_llm_fallback: bool = True
    force_llm_for_pptx: bool = True

    def modes_to_try(self, source_path: str) -> List[ConversionMode]:
        ext = Path(source_path).suffix.lower()

        # Force LLM for PPTX if configured
        if self.force_llm_for_pptx and ext == ".pptx":
            return [ConversionMode.LLM]

        # If LLM fallback is enabled, try lean first then LLM
        if self.enable_llm_fallback:
            return [ConversionMode.LEAN, ConversionMode.LLM]

        # Always try lean 
        return [ConversionMode.LEAN]

    def should_retry(self, result: ConversionResult) -> bool:
        # Retry on blank or failed results if LLM is available and we weren't already using it
        if result.mode_used == ConversionMode.LLM:
            return False
        return result.outcome in (Outcome.BLANK, Outcome.FAILED)
