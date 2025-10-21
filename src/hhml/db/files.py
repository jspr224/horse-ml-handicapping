# src/hhml/db/files.py
from __future__ import annotations

import hashlib
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine


def _sha256_of_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def register_file(
    engine: Engine,
    path: str | Path,
    track_code: str | None,
    race_date: str | None,
    *,
    provider: str,
    file_type: str,
) -> int:
    """
    Insert (or upsert by file_hash) a raw file record and return file_id.
    """
    p = Path(path).expanduser().resolve()
    file_hash = _sha256_of_file(p)

    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                insert into horse_handicapping.raw_ingest_file
                  (provider, file_type, track_code, race_date, file_name, file_hash)
                values (:provider, :file_type, :track_code, :race_date, :file_name, :file_hash)
                on conflict (file_hash) do update
                  set file_name = excluded.file_name
                returning file_id
                """
            ),
            dict(
                provider=provider,
                file_type=file_type,
                track_code=track_code,
                race_date=race_date,
                file_name=p.name,
                file_hash=file_hash,
            ),
        )
        return int(res.scalar_one())
