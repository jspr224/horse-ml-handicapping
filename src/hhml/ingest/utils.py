from __future__ import annotations

import hashlib
import json
from typing import Any


def fingerprint(row: dict[str, Any]) -> str:
    s = json.dumps(row, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def safe_int(x, default=None):
    try:
        return int(x) if x is not None and str(x).strip() != "" else default
    except Exception:
        return default


def safe_float(x, default=None):
    try:
        return float(x) if x is not None and str(x).strip() != "" else default
    except Exception:
        return default
