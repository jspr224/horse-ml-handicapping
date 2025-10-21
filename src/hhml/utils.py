# src/hhml/utils.py
from __future__ import annotations

import hashlib
import json
from typing import Any


def safe_int(v: Any) -> int | None:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    try:
        return int(s)
    except Exception:
        return None


def safe_float(v: Any) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None


def fingerprint(value: Any) -> str:
    """
    Stable hash for dicts/rows. Sort keys and compact separators to keep it stable.
    """

    def _default(o: Any) -> str:
        return str(o)

    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=_default)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def first_text(node, *paths) -> str | None:
    """
    Convenience: first non-empty text from a list of child paths.
    """
    for p in paths:
        v = node.findtext(p)
        if v is not None:
            s = str(v).strip()
            if s:
                return s
    return None
