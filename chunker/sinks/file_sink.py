from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass
class ChunkFileSink:
    root_dir: Path
    ext: str = ".md"

    def __post_init__(self) -> None:
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def write_chunk(self, *, group: str, chunk_id: str, text: str) -> Path:
        # group folder keeps dirs manageable: chunks/<json_stem>/<chunk_id>.md
        group_dir = self.root_dir / group
        group_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{chunk_id}{self.ext}"
        out_path = group_dir / filename
        out_path.write_text(text, encoding="utf-8")
        return out_path
