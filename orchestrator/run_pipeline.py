from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import yaml


@dataclass
class RunContext:
    repo_root: Path
    master_run_dir: Path
    course_root: Path          # runs/<master>/canvas/<course_id>
    processor_dir: Path        # runs/<master>/processor
    markdown_root: Path        # runs/<master>/processor/markdown
    json_output_dir: Path      # runs/<master>/canvas/<course_id>/json_output


def _run(cmd: list[str], cwd: Path, env: dict[str, str] | None = None) -> None:
    print("\n$", " ".join(cmd))
    subprocess.run(cmd, cwd=str(cwd), env=env, check=True)


def _load_yaml(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _atomic_write_json(path: Path, obj: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _compute_md_path(markdown_root: Path, raw_file_path: str) -> Path:
    # raw_file_path is like 'pages/120231.html' relative to course_root
    rel = Path(raw_file_path)
    return (markdown_root / rel).with_suffix(".md")


def _iter_json_files(json_output_dir: Path) -> Iterable[Path]:
    if not json_output_dir.exists():
        return []
    return sorted(p for p in json_output_dir.glob("*.json") if p.is_file())


def build_context(cfg: dict[str, Any], repo_root: Path, master_run_dir: Path) -> RunContext:
    course_id = str(cfg["canvas"]["course_id"])

    course_root = (master_run_dir / "canvas" / "output" / str(course_id)).resolve()
    processor_dir = (master_run_dir / "processor").resolve()
    markdown_root = (processor_dir / "markdown").resolve()
    json_output_dir = (course_root / cfg["bridge"]["json_output_dirname"]).resolve()

    return RunContext(
        repo_root=repo_root,
        master_run_dir=master_run_dir.resolve(),
        course_root=course_root,
        processor_dir=processor_dir,
        markdown_root=markdown_root,
        json_output_dir=json_output_dir,
    )


def run_crawler(cfg: dict[str, Any], repo_root: Path, master_run_dir: Path) -> None:
    canvas = cfg["canvas"]
    python = canvas.get("python") or shutil.which("python") or "python"
    crawler_script = str((repo_root / canvas["crawler_script"]).resolve())

    output_dir = (master_run_dir / "canvas").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        python,
        crawler_script,
        "--course-id", str(canvas["course_id"]),
        "--output-dir", str(output_dir),
        "--depth-limit", str(canvas.get("depth_limit", 15)),
        "--canvas-url", str(canvas["canvas_url"]),
    ]
    if canvas.get("verbose", False):
        cmd.append("--verbose")

    env = os.environ.copy()
    _run(cmd, cwd=repo_root, env=env)

def _should_skip(path: Path, skip_dirnames: set[str]) -> bool:
    # skip if any component in the path matches a skip directory name
    return any(part in skip_dirnames for part in path.parts)


def _discover_files_under(root: Path, *, skip_dirnames: set[str]) -> list[Path]:
    """
    Recursively discover files under root, excluding any files that live inside
    directories named in skip_dirnames.
    """
    files: list[Path] = []
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if _should_skip(p, skip_dirnames):
            continue
        files.append(p.resolve())
    return files

def run_conversion(cfg: dict[str, Any], repo_root: Path, ctx: RunContext) -> None:
    conv = cfg["conversion"]
    python = conv.get("python") or shutil.which("python") or "python"
    script = str((repo_root / conv["script"]).resolve())

    ctx.processor_dir.mkdir(parents=True, exist_ok=True)

    # Mirror relative to course root so markdown becomes markdown/pages/..., etc.
    source_root = ctx.course_root

    skip = set(conv.get("skip_dirnames", []))

    input_files = _discover_files_under(ctx.course_root, skip_dirnames=skip)

    if not input_files:
        print(f"No files discovered under {ctx.course_root} after skipping {sorted(skip)}")
        return

    cmd = [
        python,
        script,
        "--run-dir", str(ctx.processor_dir),
        "--source-root", str(source_root),
    ]
    if conv.get("verbose", False):
        cmd.append("--verbose")
    if not conv.get("enable_llm", True):
        cmd.append("--no-llm")
    if conv.get("model"):
        cmd += ["--model", str(conv["model"])]

    # IMPORTANT: pass explicit file paths so conversion never tries to process directories
    cmd += [str(p) for p in input_files]

    env = os.environ.copy()
    _run(cmd, cwd=repo_root, env=env)

def update_metadata(cfg: dict[str, Any], ctx: RunContext) -> Tuple[int, int]:
    bridge = cfg["bridge"]
    raw_key = bridge["raw_key"]
    legacy_key = bridge.get("legacy_key")
    md_key = bridge["md_key"]
    atomic = bool(bridge.get("atomic_write", True))
    mode = bridge.get("md_value_mode", "relative_to_master_run")

    updated = 0
    skipped = 0

    for jf in _iter_json_files(ctx.json_output_dir):
        data: Dict[str, Any] = json.loads(jf.read_text(encoding="utf-8"))

        raw = data.get(raw_key)
        if not raw and legacy_key and data.get(legacy_key):
            raw = data[legacy_key]
            data[raw_key] = raw

        if not raw:
            skipped += 1
            continue

        md_abs = _compute_md_path(ctx.markdown_root, raw)

        if not md_abs.exists():
            data[md_key] = None
        else:
            if mode == "absolute":
                data[md_key] = str(md_abs)
            elif mode == "relative_to_repo":
                data[md_key] = str(md_abs.relative_to(ctx.repo_root))
            else:
                # relative_to_master_run (recommended)
                data[md_key] = str(md_abs.relative_to(ctx.master_run_dir))

        if atomic:
            _atomic_write_json(jf, data)
        else:
            jf.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

        updated += 1

    return updated, skipped


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    cfg_path = Path(os.environ.get("PIPELINE_CONFIG", repo_root / "orchestrator" / "config.yml"))
    cfg = _load_yaml(cfg_path)

    runs_root = (repo_root / cfg["run"]["runs_root"]).resolve()
    runs_root.mkdir(parents=True, exist_ok=True)

    name = cfg["run"].get("name") or datetime.now().strftime("%Y%m%d_%H%M%S")
    master_run_dir = (runs_root / name).resolve()
    (master_run_dir / "orchestration").mkdir(parents=True, exist_ok=True)

    # Copy config used (DON'T move it)
    (master_run_dir / "orchestration" / "config_used.yml").write_text(
        cfg_path.read_text(encoding="utf-8"),
        encoding="utf-8"
    )

    ctx = build_context(cfg, repo_root, master_run_dir)

    # 1) Crawl
    run_crawler(cfg, repo_root, master_run_dir)

    # 2) Convert
    run_conversion(cfg, repo_root, ctx)

    # 3) Update metadata
    updated, skipped = update_metadata(cfg, ctx)

    summary = {
        "master_run_dir": str(master_run_dir),
        "course_root": str(ctx.course_root),
        "processor_dir": str(ctx.processor_dir),
        "markdown_root": str(ctx.markdown_root),
        "json_output_dir": str(ctx.json_output_dir),
        "updated_json_files": updated,
        "skipped_json_files": skipped,
    }
    (master_run_dir / "orchestration" / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print(f"\nMaster run dir: {master_run_dir}")
    print(f"Updated JSON files: {updated} | Skipped: {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
