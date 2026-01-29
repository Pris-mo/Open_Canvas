from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path


def setup_logging(
    name: str,
    verbose: bool,
    logs_dir: Path,
) -> logging.Logger:
    """
    Console + file logging, file is timestamped per run.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logs_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"{name}_{ts}.log"

    logger = logging.getLogger(name)

    # Avoid duplicate handlers if re-imported
    if logger.handlers:
        return logger

    logger.setLevel(level)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s %(name)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level)
    ch.setFormatter(formatter)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)

    logger.info("Logging initialized → %s", log_path)
    return logger
