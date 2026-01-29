from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class ChunkSource:
    course_id: Optional[int]
    json_file: str           # path relative to master_run
    md_file: str             # path relative to master_run
    record_type: Optional[str]
    record_id: Optional[int]
    chunk_index: int


@dataclass(frozen=True)
class ChunkRecord:
    id: str
    text: str
    source: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
