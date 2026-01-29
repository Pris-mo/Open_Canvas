from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

@dataclass(frozen=True)
class OutputConfig:
    out_dirname: str = "chunker"
    out_filename: str = "chunks.jsonl"
    write_summary: bool = True
    include_separate_metadata: bool = False
    include_source: bool = True

    write_individual_files: bool = False
    chunks_dirname: str = "chunks"
    chunk_file_ext: str = ".md"

@dataclass(frozen=True)
class FrontmatterConfig:
    enabled: bool = True
    include_keys_mode: str = "filtered"  # "filtered" | "allowlist"
    allowlist: List[str] = field(default_factory=list)
    exclude_keys: List[str] = field(default_factory=lambda: ["body"])
    exclude_key_substrings: List[str] = field(default_factory=lambda: ["file_path"])
    add_blank_line_after: bool = True
    sort_keys: bool = True


@dataclass(frozen=True)
class ChunkingConfig:
    enabled: bool = True
    md_key: str = "md_file_path"
    chunk_size: int = 1200
    chunk_overlap: int = 150
    separators: List[str] = field(default_factory=lambda: ["\n## ", "\n### ", "\n#### ", "\n\n", "\n", " ", ""])
    skip_dirnames: List[str] = field(default_factory=lambda: ["json_output", "locked"])
    frontmatter: FrontmatterConfig = field(default_factory=FrontmatterConfig)
    output: OutputConfig = field(default_factory=OutputConfig)


def load_config(path: Path) -> Dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def parse_chunking_config(cfg: Dict[str, Any]) -> ChunkingConfig:
    c = cfg.get("chunking", {}) or {}

    fm = c.get("frontmatter", {}) or {}
    out = c.get("output", {}) or {}

    fm_cfg = FrontmatterConfig(
        enabled=bool(fm.get("enabled", True)),
        include_keys_mode=str(fm.get("include_keys_mode", "filtered")),
        allowlist=list(fm.get("allowlist", []) or []),
        exclude_keys=list(fm.get("exclude_keys", ["body"]) or ["body"]),
        exclude_key_substrings=list(fm.get("exclude_key_substrings", ["file_path"]) or ["file_path"]),
        add_blank_line_after=bool(fm.get("add_blank_line_after", True)),
        sort_keys=bool(fm.get("sort_keys", True)),
    )

    out_cfg = OutputConfig(
        out_dirname=str(out.get("out_dirname", "chunker")),
        out_filename=str(out.get("out_filename", "chunks.jsonl")),
        write_summary=bool(out.get("write_summary", True)),
        include_separate_metadata=bool(out.get("include_separate_metadata", False)),
        include_source=bool(out.get("include_source", True)),

        write_individual_files=bool(out.get("write_individual_files", False)),
        chunks_dirname=str(out.get("chunks_dirname", "chunks")),
        chunk_file_ext=str(out.get("chunk_file_ext", ".md")),
    )

    return ChunkingConfig(
        enabled=bool(c.get("enabled", True)),
        md_key=str(c.get("md_key", "md_file_path")),
        chunk_size=int(c.get("chunk_size", 1200)),
        chunk_overlap=int(c.get("chunk_overlap", 150)),
        separators=list(c.get("separators", ["\n## ", "\n### ", "\n#### ", "\n\n", "\n", " ", ""]) or []),
        skip_dirnames=list(c.get("skip_dirnames", ["json_output", "locked"]) or []),
        frontmatter=fm_cfg,
        output=out_cfg,
    )
