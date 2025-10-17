from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from hhml.config import POSTGRES_URL


def get_engine(echo: bool = False) -> Engine:
    if not POSTGRES_URL:
        raise RuntimeError("POSTGRES_URL is not set. Populate .env or environment variables.")
    return create_engine(POSTGRES_URL, echo=echo, pool_pre_ping=True)
