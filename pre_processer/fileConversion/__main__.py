from __future__ import annotations

from . import AppConfig
from .converters import MarkItDownConverter
from .logging_utils import setup_logging
from .policies import FallbackPolicy
from .pipeline import Pipeline
from .sinks import MarkdownSink, JsonlSink


def main() -> int:
    # Minimal default entry point: no args parsing here.
    # Prefer running via pre_processer/run_conversion.py for CLI flags.
    cfg = AppConfig()
    run_dir = cfg.runs_root  # will be refined in run_conversion.py
    logs_dir = run_dir / "logs"

    logger = setup_logging("fileConversion", cfg.verbose, logs_dir)

    converter = MarkItDownConverter(
        llm_api_key=cfg.get_openai_api_key(),
        llm_model=cfg.llm_model,
        enable_llm=cfg.enable_llm_fallback,
        logger=logger,
    )

    policy = FallbackPolicy(
        enable_llm_fallback=cfg.enable_llm_fallback,
        force_llm_for_pptx=cfg.force_llm_for_pptx,
    )

    md_sink = MarkdownSink(out_dir=(run_dir / "markdown"), logger=logger)
    jsonl_sink = JsonlSink(out_path=(run_dir / "ledger.jsonl"), logger=logger)

    pipeline = Pipeline(
        config=cfg,
        converter=converter,
        policy=policy,
        logger=logger,
        md_sink=md_sink,
        jsonl_sink=jsonl_sink,
    )

    # No default inputs here.
    logger.info("No paths provided. Use run_conversion.py to pass inputs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
