from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple
from types import SimpleNamespace

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


def _resolve_python(cfg_section: dict[str, Any] | None, repo_root: Path) -> str:
    if cfg_section and cfg_section.get("python"):
        return str(cfg_section["python"])

    env_py = os.environ.get("VENV_PY")
    if env_py and Path(env_py).is_file():
        return env_py

    venv_py = repo_root / ".venv" / "bin" / "python"
    if venv_py.is_file():
        return str(venv_py)

    return shutil.which("python") or "python"


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

    # Treat this as a *module path* for `python -m`
    # e.g. "canvas_crawler.cli" or "canvas_crawler.canvas_crawler"
    crawler_module = canvas.get("crawler_script") or "canvas_crawler.cli"

    output_dir = (master_run_dir / "canvas").resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        python,
        "-m", crawler_module,          # 👈 key change: module, not script path
        "--course-id", str(canvas["course_id"]),
        "--output-dir", str(output_dir),
        "--depth-limit", str(canvas.get("depth_limit", 15)),
        "--canvas-url", str(canvas["canvas_url"]),
    ]

    token = canvas.get("token")
    if token:
        cmd += ["--token", str(token)]

    if canvas.get("verbose", False):
        cmd.append("--verbose")

    env = os.environ.copy()
    _run(cmd, cwd=repo_root, env=env)


def _should_skip(path: Path, root: Path, skip_dirnames: set[str]) -> bool:
    rel = path.relative_to(root)
    return any(part in skip_dirnames for part in rel.parts)


def _discover_files_under(
    root: Path,
    *,
    skip_dirnames: set[str],
    include_dirnames: set[str] | None = None,
) -> list[Path]:
    """
    Recursively discover files under root, excluding any files that live inside
    directories named in skip_dirnames.

    If include_dirnames is provided, only include files whose *top-level* folder
    (relative to root) is in include_dirnames. Root-level files (no parent dir)
    are always included.
    """
    files: list[Path] = []
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if _should_skip(p, root, skip_dirnames):
            continue

        rel = p.relative_to(root)

        # Root-level files are included by default
        if len(rel.parts) == 1:
            files.append(p.resolve())
            continue

        top = rel.parts[0]
        if include_dirnames is not None:
            if top not in include_dirnames:
                continue

        files.append(p.resolve())

    return files

def run_conversion(cfg: dict[str, Any], repo_root: Path, ctx: RunContext) -> None:
    conv = cfg["conversion"]
    python = conv.get("python") or shutil.which("python") or "python"

    # Treat this as either a module path or a relative script path.
    # Default: module path for installed package usage.
    conv_target = conv.get("script") or "pre_processer.run_conversion"

    ctx.processor_dir.mkdir(parents=True, exist_ok=True)

    # Mirror relative to course root so markdown becomes markdown/pages/..., etc.
    source_root = ctx.course_root

    # Always skip these
    skip = set(conv.get("skip_dirnames", [])) | {"locked", "json_output"}

    # Optional include list
    include_dirnames: set[str] | None = None
    raw_include = conv.get("include_dirnames")
    if raw_include:
        include_dirnames = {str(x).strip() for x in raw_include if str(x).strip()}
        # never allow these even if user includes them
        include_dirnames -= {"locked", "json_output"}

    input_files = _discover_files_under(
        ctx.course_root,
        skip_dirnames=skip,
        include_dirnames=include_dirnames,
    )

    if not input_files:
        msg = f"No files discovered under {ctx.course_root} after skipping {sorted(skip)}"
        if include_dirnames is not None:
            msg += f" and including only {sorted(include_dirnames)}"
        print(msg)
        return

    # Decide whether this is a module or a path
    is_module_like = (
        "/" not in conv_target
        and "\\" not in conv_target
        and not conv_target.endswith(".py")
    )

    if is_module_like:
        # e.g. "pre_processer.run_conversion"
        cmd = [
            python,
            "-m",
            conv_target,
            "--run-dir", str(ctx.processor_dir),
            "--source-root", str(source_root),
        ]
    else:
        # Treat as a relative script path under repo_root
        script_path = (repo_root / conv_target).resolve()
        cmd = [
            python,
            str(script_path),
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

def run_chunking(cfg: dict[str, Any], repo_root: Path, ctx: RunContext, cfg_path: Path) -> None:
    ch = cfg.get("chunking", {}) or {}
    if not ch.get("enabled", False):
        print("Chunking disabled (chunking.enabled=false). Skipping.")
        return

    python = _resolve_python(ch, repo_root)

    cmd = [
        python,
        "-m", "chunker.cli",
        "--master-run", str(ctx.master_run_dir),
        "--json-output", str(ctx.json_output_dir),
    ]

    course_id = cfg.get("canvas", {}).get("course_id")
    if course_id is not None:
        cmd += ["--course-id", str(course_id)]

    if ch.get("include_separate_metadata", False):
        cmd.append("--include-separate-metadata")

    if ch.get("write_chunk_files", False):
        cmd.append("--write-chunk-files")

    env = os.environ.copy()
    env["PIPELINE_CONFIG"] = str(cfg_path)

    _run(cmd, cwd=repo_root, env=env)

def _resolve_runs_root(runs_root_arg: str | Path, repo_root: Path) -> Path:
    """
    Resolve runs_root based on whether it's absolute or relative.

    - If runs_root_arg is an absolute path, use it as-is.
    - Otherwise, treat it as relative to repo_root.
    """
    p = Path(runs_root_arg)
    if p.is_absolute():
        return p.resolve()
    return (repo_root / p).resolve()

def run_filtering_stage(cfg: dict[str, Any], repo_root: Path, ctx: RunContext) -> Dict[str, Any] | None:
    """
    Run the filterer on the crawled Canvas course directory, if enabled.

    Config shape (YAML or CLI-built):

    filtering:
      enabled: true            # default False if omitted
      script: "filterer.cli"   # module path or script path
      python: "/path/to/venv/bin/python"  # optional
      exclude_csv: "path/to/exclude.csv"
      min_token_count: 25
      dedupe: true
      dry_run: false
      title_blacklist: "midterm,exam,solution"
      include_dirnames: ["pages", "files"]  # or comma-separated string
      skip_dirnames: ["some_dir"]
      log_removed: false
      log_removed_to: "runs/.../removed.jsonl"
      max_removed_in_summary: 2000
      summary_json: "runs/.../filter_summary.json"
    """
    filtering_cfg = cfg.get("filtering") or {}
    if not filtering_cfg.get("enabled", False):
        print("Filtering disabled (filtering.enabled=false). Skipping.")
        return None

    python = _resolve_python(filtering_cfg, repo_root)

    # Module path or script path, like other stages
    target = filtering_cfg.get("script") or "filterer.cli"

    # Where filterer will write its own summary JSON
    summary_json_path = filtering_cfg.get(
        "summary_json",
        str(ctx.master_run_dir / "orchestration" / "filter_summary.json"),
    )

    def _normalise_csvish(value: Any) -> str | None:
        """
        Turn list/tuple/set or string into a single comma-separated string,
        or None if empty.
        """
        if value is None:
            return None
        if isinstance(value, (list, tuple, set)):
            parts = [str(v).strip() for v in value if str(v).strip()]
            return ",".join(parts) if parts else None
        s = str(value).strip()
        return s or None

    # Normalise include/skip/title lists for the CLI
    include_dirnames = _normalise_csvish(filtering_cfg.get("include_dirnames"))
    skip_dirnames = _normalise_csvish(filtering_cfg.get("skip_dirnames"))
    title_blacklist = _normalise_csvish(filtering_cfg.get("title_blacklist"))

    # Decide module vs script path
    is_module_like = (
        "/" not in target
        and "\\" not in target
        and not target.endswith(".py")
    )

    if is_module_like:
        cmd = [
            python,
            "-m",
            target,
            "--course-root",
            str(ctx.course_root),
            "--summary-json",
            str(summary_json_path),
        ]
    else:
        script_path = (repo_root / target).resolve()
        cmd = [
            python,
            str(script_path),
            "--course-root",
            str(ctx.course_root),
            "--summary-json",
            str(summary_json_path),
        ]

    # Map config options -> CLI flags, only if set
    if filtering_cfg.get("exclude_csv"):
        cmd += ["--exclude-csv", str(filtering_cfg["exclude_csv"])]

    if title_blacklist:
        cmd += ["--title-blacklist", title_blacklist]

    if filtering_cfg.get("min_token_count") is not None:
        cmd += ["--min-token-count", str(int(filtering_cfg["min_token_count"]))]

    # dedupe: filterer's default is dedupe=True; pass --no-dedupe if False
    if filtering_cfg.get("dedupe", True) is False:
        cmd.append("--no-dedupe")

    if include_dirnames:
        cmd += ["--include-dirnames", include_dirnames]

    if skip_dirnames:
        cmd += ["--skip-dirnames", skip_dirnames]

    if filtering_cfg.get("dry_run", False):
        cmd.append("--dry-run")

    if filtering_cfg.get("log_removed", False):
        cmd.append("--log-removed")

    if filtering_cfg.get("log_removed_to"):
        cmd += ["--log-removed-to", str(filtering_cfg["log_removed_to"])]

    if filtering_cfg.get("max_removed_in_summary") is not None:
        cmd += [
            "--max-removed-in-summary",
            str(int(filtering_cfg["max_removed_in_summary"])),
        ]

    env = os.environ.copy()
    print("::STEP:: Filtering crawled course files (pre-conversion)", flush=True)
    _run(cmd, cwd=repo_root, env=env)

    # Try to read the summary JSON the filterer wrote so we can embed in orchestration summary
    summary_path = Path(summary_json_path)
    if summary_path.exists():
        try:
            return json.loads(summary_path.read_text(encoding="utf-8"))
        except Exception:
            return {"error": f"Failed to parse filter summary JSON at {summary_json_path}"}
    return None


def run_pipeline(cfg: dict[str, Any], repo_root: Path, cfg_path: Path | None = None) -> int:
    runs_root = _resolve_runs_root(cfg["run"]["runs_root"], repo_root)
    runs_root.mkdir(parents=True, exist_ok=True)

    name = cfg["run"].get("name") or datetime.now().strftime("%Y%m%d_%H%M%S")
    master_run_dir = (runs_root / name).resolve()
    (master_run_dir / "orchestration").mkdir(parents=True, exist_ok=True)

    # Copy config used if we have a cfg_path (YAML). Otherwise write JSON snapshot.
    if cfg_path and cfg_path.exists():
        (master_run_dir / "orchestration" / "config_used.yml").write_text(
            cfg_path.read_text(encoding="utf-8"),
            encoding="utf-8"
        )
    else:
        (master_run_dir / "orchestration" / "config_used.json").write_text(
            json.dumps(cfg, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

    ctx = build_context(cfg, repo_root, master_run_dir)


    
    # 1) Crawl
    print("::STEP:: Step 1 of 5: Crawling the Canvas Course and Gathering Files", flush=True)
    run_crawler(cfg, repo_root, master_run_dir)

    # 2) Filtering (NEW)
    print("::STEP:: Step 2 of 6: Filtering crawled course files", flush=True)
    filter_summary = run_filtering_stage(cfg, repo_root, ctx)

    # 2) Convert
    print("::STEP:: Step 2 of 5: Converting Files from source format to markdown", flush=True)
    run_conversion(cfg, repo_root, ctx)

    # 3) Update metadata
    print("::STEP:: Step 3 of 5: Adding and verifying files metadata", flush=True)
    updated, skipped = update_metadata(cfg, ctx)

    # 4) Chunking
    print("::STEP:: Step 4 of 5: Chunking the markdown files, preparing to upload", flush=True)
    run_chunking(cfg, repo_root, ctx, cfg_path or (repo_root / "orchestrator" / "config.yml"))
    print("::STEP:: Step 5 of 5: Course Provisioning", flush=True)

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



def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    cfg_path = Path(os.environ.get("PIPELINE_CONFIG", repo_root / "orchestrator" / "config.yml"))
    cfg = _load_yaml(cfg_path)
    return run_pipeline(cfg, repo_root, cfg_path)



if __name__ == "__main__":
    raise SystemExit(main())
