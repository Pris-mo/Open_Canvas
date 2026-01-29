from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from langchain_text_splitters import RecursiveCharacterTextSplitter

from .config import ChunkingConfig
from .frontmatter import apply_frontmatter, filter_metadata
from .io import iter_json_records, rel_to, resolve_md_path
from .sinks.jsonl_sink import JsonlSink
from .sinks.file_sink import ChunkFileSink

@dataclass
class ChunkingSummary:
    total_json_files: int = 0
    records_with_md: int = 0
    missing_md_file: int = 0
    blank_md_file: int = 0
    total_chunks_written: int = 0


def _infer_record_id(rec: Dict[str, Any]) -> Optional[int]:
    # Prefer 'id' if it's an int; else None
    rid = rec.get("id")
    return rid if isinstance(rid, int) else None


def _infer_record_type(rec: Dict[str, Any]) -> Optional[str]:
    t = rec.get("type")
    return t if isinstance(t, str) else None


def run_chunking(
    *,
    master_run_dir: Path,
    json_output_dir: Path,
    course_id: Optional[int],
    cfg: ChunkingConfig,
) -> Tuple[Path, ChunkingSummary]:
    out_dir = master_run_dir / cfg.output.out_dirname
    out_path = out_dir / cfg.output.out_filename
    out_dir.mkdir(parents=True, exist_ok=True)

    # reset output file each run (v1 behavior)
    if out_path.exists():
        out_path.unlink()

    sink = JsonlSink(out_path=out_path)

    file_sink = None
    if cfg.output.write_individual_files:
        chunks_dir = out_dir / cfg.output.chunks_dirname
        file_sink = ChunkFileSink(root_dir=chunks_dir, ext=cfg.output.chunk_file_ext)

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=cfg.chunk_size,
        chunk_overlap=cfg.chunk_overlap,
        separators=cfg.separators,
    )

    summary = ChunkingSummary()

    for jf, rec in iter_json_records(json_output_dir):
        summary.total_json_files += 1

        md_rel = rec.get(cfg.md_key)
        if not md_rel or not isinstance(md_rel, str):
            continue

        summary.records_with_md += 1
        md_abs = resolve_md_path(master_run_dir, md_rel)

        if not md_abs.exists():
            summary.missing_md_file += 1
            continue

        text = md_abs.read_text(encoding="utf-8", errors="replace")
        if not text.strip():
            summary.blank_md_file += 1
            continue

        # Filter metadata and apply frontmatter (Option A)
        meta = filter_metadata(rec, cfg.frontmatter)

        chunks = splitter.split_text(text)
        json_stem = jf.stem  # stable base id: e.g. page_120231

        json_file_rel = rel_to(jf, master_run_dir)
        md_file_rel = md_rel

        record_type = _infer_record_type(rec)
        record_id = _infer_record_id(rec)

        for i, chunk_text in enumerate(chunks):
            chunk_id = f"{json_stem}#{i:04d}"
            final_text = apply_frontmatter(chunk_text, meta, cfg.frontmatter)

            chunk_file_rel = None
            if file_sink is not None:
                chunk_path = file_sink.write_chunk(
                    group=json_stem,
                    chunk_id=chunk_id,
                    text=final_text,
                )
                try:
                    chunk_file_rel = str(chunk_path.relative_to(master_run_dir))
                except Exception:
                    chunk_file_rel = str(chunk_path)

            obj: Dict[str, Any] = {
                "id": chunk_id,
                "text": final_text,
            }

            if chunk_file_rel is not None:
                obj["chunk_file_path"] = chunk_file_rel

            if cfg.output.include_source:
                obj["source"] = {
                    "course_id": course_id,
                    "json_file": json_file_rel,
                    "md_file": md_file_rel,
                    "record_type": record_type,
                    "record_id": record_id,
                    "chunk_index": i,
                }

            if cfg.output.include_separate_metadata:
                obj["metadata"] = meta

            sink.write_line(obj)
            summary.total_chunks_written += 1


    if cfg.output.write_summary:
        (out_dir / "chunk_summary.json").write_text(
            json.dumps(
                {
                    "out_path": str(rel_to(out_path, master_run_dir)),
                    "course_id": course_id,
                    "json_output_dir": str(rel_to(json_output_dir, master_run_dir)),
                    "md_key": cfg.md_key,
                    "chunk_size": cfg.chunk_size,
                    "chunk_overlap": cfg.chunk_overlap,
                    "frontmatter_enabled": cfg.frontmatter.enabled,
                    "include_separate_metadata": cfg.output.include_separate_metadata,
                    "include_source": cfg.output.include_source,
                    "counts": {
                        "total_json_files": summary.total_json_files,
                        "records_with_md": summary.records_with_md,
                        "missing_md_file": summary.missing_md_file,
                        "blank_md_file": summary.blank_md_file,
                        "total_chunks_written": summary.total_chunks_written,
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    return out_path, summary
