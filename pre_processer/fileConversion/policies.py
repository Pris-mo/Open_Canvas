from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

from .schema import AttemptStep, ConversionMode, Engine, ConversionResult, Outcome


@dataclass(frozen=True)
class FallbackPolicy:
    enable_markitdown_llm: bool = True
    force_llm_for_pptx: bool = False  # optional, keep if you still want it

    def steps_to_try(self, path: str) -> List[AttemptStep]:
        p = Path(path)
        ext = p.suffix.lower()

        # If you still want pptx to force LLM for MarkItDown (optional)
        if self.force_llm_for_pptx and ext == ".pptx":
            if self.enable_markitdown_llm:
                return [
                    AttemptStep(Engine.DOCLING, ConversionMode.LEAN),
                    AttemptStep(Engine.MARKITDOWN, ConversionMode.LLM),
                ]
            return [
                AttemptStep(Engine.DOCLING, ConversionMode.LEAN),
                AttemptStep(Engine.MARKITDOWN, ConversionMode.LEAN),
            ]

        steps = [
            AttemptStep(Engine.DOCLING, ConversionMode.LEAN),
            AttemptStep(Engine.MARKITDOWN, ConversionMode.LEAN),
        ]
        if self.enable_markitdown_llm:
            steps.append(AttemptStep(Engine.MARKITDOWN, ConversionMode.LLM))
        return steps

    def should_continue(self, result: ConversionResult) -> bool:
        # Continue trying fallbacks if we didn't get OK.
        return result.outcome != Outcome.OK
