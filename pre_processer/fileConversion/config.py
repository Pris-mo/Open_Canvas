from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class AppConfig:
    # Where to put run outputs (logs, markdown, ledgers)
    runs_root: Path = Path("runs")

    # LLM settings
    enable_llm_fallback: bool = True
    llm_model: str = "gpt-4o"
    openai_api_key_env: str = "OPENAI_API_KEY"

    # Policy knobs
    force_llm_for_pptx: bool = True

    # Logging
    verbose: bool = False

    def get_openai_api_key(self) -> Optional[str]:
        return os.getenv(self.openai_api_key_env)
