from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional
import logging

from .config import AppConfig
from .logging_utils import setup_logging
from .policies import FallbackPolicy
from .schema import AttemptStep, ConversionMode, ConversionResult, Engine, Outcome, RunSummary
from .converters.docling_converter import DoclingConverter
from .converters.markitdown_converter import MarkItDownConverter
from .sinks.markdown_sink import MarkdownSink
from .sinks.jsonl_sink import JsonlSink


@dataclass
class Pipeline:
    config: AppConfig
    logger: logging.Logger
    docling: DoclingConverter
    markitdown: MarkItDownConverter
    policy: FallbackPolicy
    md_sink: MarkdownSink
    jsonl_sink: JsonlSink
    run_dir: Path

    @classmethod
    def from_config(cls, config: AppConfig, *, run_dir: Optional[Path] = None):
        if run_dir is None:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            run_dir = config.runs_root / ts

        logs_dir = run_dir / "logs"
        md_dir = run_dir / "markdown"
        ledger_path = run_dir / "ledger.jsonl"
        run_dir.mkdir(parents=True, exist_ok=True)

        logger = setup_logging("fileConversion", config.verbose, logs_dir)
        logger.info("Run directory → %s", run_dir)

        # Converters
        docling = DoclingConverter(logger=logger)

        markitdown = MarkItDownConverter(
            llm_api_key=config.get_openai_api_key(),
            llm_model=config.llm_model,
            enable_llm=config.enable_llm_fallback,
            logger=logger,
        )

        policy = FallbackPolicy(
            enable_markitdown_llm=config.enable_llm_fallback,
            force_llm_for_pptx=config.force_llm_for_pptx,
        )

        md_sink = MarkdownSink(out_dir=md_dir, logger=logger)
        jsonl_sink = JsonlSink(out_path=ledger_path, logger=logger)

        return cls(
            config=config,
            logger=logger,
            docling=docling,
            markitdown=markitdown,
            policy=policy,
            md_sink=md_sink,
            jsonl_sink=jsonl_sink,
            run_dir=run_dir,
        )

    def _run_step(self, step: AttemptStep, path: str, attempt: int) -> ConversionResult:
        if step.engine == Engine.DOCLING:
            res = self.docling.convert(path, step.mode, attempt=attempt)
        elif step.engine == Engine.MARKITDOWN:
            res = self.markitdown.convert(path, step.mode, attempt=attempt)
        else:
            raise ValueError(f"Unknown engine: {step.engine}")

        # Ensure engine/mode are always recorded consistently
        res.meta = dict(res.meta or {})
        res.meta["engine"] = step.engine.value
        res.meta["mode"] = step.mode.value
        return res

    def run(self, paths: Iterable[str]) -> RunSummary:
        summary = RunSummary()

        for p in paths:
            summary.total += 1
            self.logger.info("Processing: %s", p)

            steps = self.policy.steps_to_try(p)
            final: Optional[ConversionResult] = None

            for i, step in enumerate(steps, start=1):
                res = self._run_step(step, p, attempt=i)
                self.jsonl_sink.append(res)

                self.logger.info(
                    "Attempt %d/%d | engine=%s | mode=%s | outcome=%s | ms=%s | path=%s",
                    i, len(steps), step.engine.value, step.mode.value, res.outcome.value, res.duration_ms, p
                )

                final = res
                if not self.policy.should_continue(res):
                    break

            if final is None:
                summary.failed += 1
                continue

            if final.outcome == Outcome.OK:
                summary.ok += 1
                self.md_sink.write(final)
            elif final.outcome == Outcome.BLANK:
                summary.blank += 1
            else:
                summary.failed += 1

        self.logger.info(
            "Run summary | total=%d ok=%d blank=%d failed=%d",
            summary.total, summary.ok, summary.blank, summary.failed
        )
        return summary
