import os
import csv
import logging
from logging.handlers import RotatingFileHandler
from typing import List, Tuple, Dict
from dotenv import load_dotenv
from markitdown import MarkItDown
from openai import OpenAI

# === Logging setup ===
def setup_logging(log_dir: str = "logs",
                  log_file: str = "conversion.log",
                  level: str = None) -> logging.Logger:
    """
    Configure a root logger with both console and rotating file handlers.
    LOG_LEVEL env var (DEBUG|INFO|WARNING|ERROR) overrides default if present.
    """
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, log_file)

    logger = logging.getLogger("markitdown_pipeline")
    if logger.handlers:
        return logger  # already configured

    # Resolve log level
    level_name = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    lvl = getattr(logging, level_name, logging.INFO)

    logger.setLevel(lvl)

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s:%(lineno)d | %(message)s"
    )

    # Console
    sh = logging.StreamHandler()
    sh.setLevel(lvl)
    sh.setFormatter(fmt)

    # Rotating file
    fh = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    fh.setLevel(lvl)
    fh.setFormatter(fmt)

    logger.addHandler(sh)
    logger.addHandler(fh)

    logger.debug("Logger initialized. Level=%s, file=%s", level_name, log_path)
    return logger

logger = setup_logging()

# === Env & client setup ===
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

_llm_client = None
def get_llm_client():
    global _llm_client
    if _llm_client is None:
        if not OPENAI_API_KEY:
            logger.error("OPENAI_API_KEY is not set in your .env")
            raise RuntimeError("OPENAI_API_KEY is not set in your .env")
        _llm_client = OpenAI(api_key=OPENAI_API_KEY)
        logger.debug("OpenAI client initialized")
    return _llm_client

def _mk_converter(use_llm: bool):
    if use_llm:
        client = get_llm_client()
        logger.debug("Creating MarkItDown with LLM model (gpt-4o)")
        return MarkItDown(llm_client=client, llm_model="gpt-4o")
    else:
        logger.debug("Creating MarkItDown without plugins (lean mode)")
        return MarkItDown(enable_plugins=False)

def _is_blank(result) -> bool:
    try:
        txt = (result.text_content or "").strip()
        return len(txt) == 0
    except Exception:
        return True

def convert_file(path: str) -> Tuple[str, bool]:
    """
    Convert a file to Markdown:
      - If it's a .pptx, force LLM mode.
      - Otherwise try non-LLM first; if blank/empty, retry with LLM.
    Returns (markdown_text, used_llm: bool)
    """
    ext = os.path.splitext(path)[1].lower()
    force_llm = (ext == ".pptx")
    logger.debug("convert_file start | path=%s | ext=%s | force_llm=%s", path, ext, force_llm)

    # First attempt
    md = _mk_converter(use_llm=force_llm)
    try:
        result = md.convert(path)
        logger.debug("Initial convert() completed | force_llm=%s", force_llm)
    except Exception as e:
        logger.warning("Initial convert() raised: %s | path=%s | force_llm=%s", e, path, force_llm)
        # If we weren't using LLM, try LLM as a fallback on exception as well
        if not force_llm:
            logger.info("Retrying with LLM after exception | path=%s", path)
            md_llm = _mk_converter(use_llm=True)
            result = md_llm.convert(path)  # let this raise if it fails
            return (result.text_content or "", True)
        raise  # re-raise if already in force_llm mode

    # If blank and we haven't tried LLM yet, fallback to LLM
    if _is_blank(result) and not force_llm:
        logger.info("Blank result without LLM, retrying with LLM | path=%s", path)
        md_llm = _mk_converter(use_llm=True)
        result2 = md_llm.convert(path)
        return (result2.text_content or "", True)

    return (result.text_content or "", force_llm)

def _filesize(path: str) -> str:
    try:
        b = os.path.getsize(path)
        return f"{b} B"
    except Exception:
        return "unknown"

def _write_failures_csv(failures: List[Dict[str, str]], out_dir: str = "logs") -> str:
    if not failures:
        return ""
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "failed_conversions.csv")
    # Append if exists; write header only when creating
    write_header = not os.path.exists(out_path)

    with open(out_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["path", "error"])
        if write_header:
            w.writeheader()
        for row in failures:
            w.writerow({"path": row.get("path", ""), "error": row.get("error", "")})

    logger.info("Wrote %d failure record(s) to %s", len(failures), out_path)
    return out_path

def process_paths(paths: List[str]) -> Dict[str, int]:
    total = 0
    ok = 0
    blank = 0
    failures: List[Dict[str, str]] = []

    for p in paths:
        total += 1
        logger.info("Processing: %s | size=%s", p, _filesize(p))
        if not os.path.exists(p):
            msg = "File does not exist"
            logger.error("❌ %s: %s", msg, p)
            failures.append({"path": p, "error": msg})
            continue

        try:
            text, used_llm = convert_file(p)
            logger.info("Converted | used_llm=%s | path=%s", used_llm, p)

            if text.strip():
                ok += 1
                logger.debug("Non-empty output | chars=%d | path=%s", len(text), p)
                # (Optional) If you want to persist text somewhere, write it here.
            else:
                blank += 1
                logger.warning("⚠️ Conversion returned no text | path=%s", p)

        except Exception as e:
            logger.exception("❌ Failed to convert %s", p)  # includes traceback in file log
            failures.append({"path": p, "error": str(e)})

    csv_path = _write_failures_csv(failures) if failures else ""
    logger.info("Run summary | total=%d ok=%d blank=%d failed=%d", total, ok, blank, len(failures))
    if csv_path:
        logger.info("Failed file ledger: %s", csv_path)

    return {"total": total, "ok": ok, "blank": blank, "failed": len(failures)}

if __name__ == "__main__":
    paths = []
    paths += [
        r'C:\Users\dthop\Local_Documents\OpenCanvas\Open_Canvas\pre_processer\test_files\Anderson 1984.pdf',
        r'C:\Users\dthop\Local_Documents\OpenCanvas\Open_Canvas\pre_processer\test_files\NFWReport2024_12.20.24.pdf', # ocr already applied
        r'C:\Users\dthop\Local_Documents\OpenCanvas\Open_Canvas\pre_processer\test_files\aussieactive-1brtlzDq-o8-unsplash.jpg',
        r'C:\Users\dthop\Local_Documents\OpenCanvas\Open_Canvas\pre_processer\test_files\Cognitive Searvices and Content Intelligence.pptx',
    ]
    summary = process_paths(paths)
    # Keep a one-line summary at the end of console output:
    print(f"Summary: {summary}")
