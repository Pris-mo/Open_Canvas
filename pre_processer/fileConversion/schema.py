from __future__ import annotations

from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional


class Engine(str, Enum):
    DOCLING = "docling"
    MARKITDOWN = "markitdown"


class ConversionMode(str, Enum):
    LEAN = "lean"
    LLM = "llm"


@dataclass(frozen=True)
class AttemptStep:
    engine: Engine
    mode: ConversionMode


class Outcome(str, Enum):
    OK = "ok"
    BLANK = "blank"
    FAILED = "failed"


@dataclass
class ConversionResult:
    source_path: str
    mode_used: ConversionMode
    outcome: Outcome
    engine_used: Engine

    # Core output
    markdown: str = ""

    # Useful diagnostics / future-proofing
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    artifacts: Dict[str, Any] = field(default_factory=dict)

    # Runtime info
    duration_ms: Optional[int] = None
    attempt: int = 1  # 1-based attempt count


@dataclass
class RunPaths:
    run_dir: Path
    logs_dir: Path
    out_md_dir: Path
    out_jsonl_path: Path


@dataclass
class RunSummary:
    total: int = 0
    ok: int = 0
    blank: int = 0
    failed: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
