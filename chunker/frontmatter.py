from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import yaml

from .config import FrontmatterConfig


def filter_metadata(record: Dict[str, Any], cfg: FrontmatterConfig) -> Dict[str, Any]:
    """
    Apply exclusion rules:
      - remove keys in exclude_keys (case-insensitive)
      - remove keys containing any exclude_key_substrings (case-insensitive)
      - if allowlist mode, include only allowlist keys
    Also: keep primitives only (str/int/float/bool/None) to keep frontmatter clean.
    """
    out: Dict[str, Any] = {}

    exclude_keys = {k.lower() for k in cfg.exclude_keys}
    exclude_subs = tuple(s.lower() for s in cfg.exclude_key_substrings)
    allow = set(cfg.allowlist) if cfg.include_keys_mode == "allowlist" else None

    for k, v in record.items():
        lk = k.lower()

        if lk in exclude_keys:
            continue
        if any(sub in lk for sub in exclude_subs):
            continue
        if allow is not None and k not in allow:
            continue

        if isinstance(v, (str, int, float, bool)) or v is None:
            out[k] = v

    return out


def render_frontmatter(meta: Dict[str, Any], cfg: FrontmatterConfig) -> str:
    if not cfg.enabled:
        return ""
    if not meta:
        return ""

    dumped = yaml.safe_dump(
        meta,
        sort_keys=cfg.sort_keys,
        allow_unicode=True,
    ).strip("\n")

    if not dumped.strip():
        return ""

    fm = f"---\n{dumped}\n---\n"
    if cfg.add_blank_line_after:
        fm += "\n"
    return fm


def apply_frontmatter(chunk_text: str, meta: Dict[str, Any], cfg: FrontmatterConfig) -> str:
    fm = render_frontmatter(meta, cfg)
    return fm + chunk_text if fm else chunk_text
