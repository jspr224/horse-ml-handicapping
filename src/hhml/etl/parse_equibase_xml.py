
from __future__ import annotations
import argparse
import json
from pathlib import Path
from typing import Iterator, Dict, Any
from lxml import etree
from tqdm import tqdm
from sqlalchemy import text
from hhml.db.connect import get_engine

def iter_xml_files(root: Path, kind: str) -> Iterator[Path]:
    exts = (".xml", ".XML")
    for p in root.rglob("*"):
        if p.suffix in exts and kind.lower() in p.parent.name.lower():
            yield p

def parse_race_stub(doc: etree._ElementTree) -> Dict[str, Any]:
    # TODO: Replace with full mapping
    root = doc.getroot()
    track_code = root.findtext("TRACK/CODE") or "UNK"
    race_date = root.findtext("RACE_DATE") or "1970-01-01"
    race_num = int(root.findtext("RACE/NUMBER") or 0)
    race_id = f"{track_code}_{race_date}_{race_num:02d}"
    return {
        "race_id": race_id,
        "track_code": track_code,
        "race_date": race_date,
        "race_num": race_num,
        "surface": root.findtext("RACE/SURFACE"),
        "distance_yards": None,
        "field_size": None,
        "condition_text": root.findtext("RACE/CONDITION"),
        "track_condition": root.findtext("RACE/TRACK_CONDITION"),
    }

def upsert_race(conn, race: Dict[str, Any]) -> None:
    sql = text("""        insert into horse_handicapping.race (race_id, track_code, race_date, race_num, surface, distance_yards, field_size, condition_text, track_condition)
        values (:race_id, :track_code, :race_date, :race_num, :surface, :distance_yards, :field_size, :condition_text, :track_condition)
        on conflict (race_id) do update set
          surface = excluded.surface,
          distance_yards = excluded.distance_yards,
          field_size = excluded.field_size,
          condition_text = excluded.condition_text,
          track_condition = excluded.track_condition
    """)
    conn.execute(sql, race)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_dir", required=True, help="Root path with XML folders (e.g., data/raw/2023)")
    ap.add_argument("--kind", choices=["results", "charts", "pp"], default="results")
    ap.add_argument("--limit", type=int, default=0, help="Parse only the first N files (0 for all)")
    ap.add_argument("--echo", action="store_true", help="SQL echo")
    args = ap.parse_args()

    in_path = Path(args.in_dir).resolve()
    engine = get_engine(echo=args.echo)

    files = list(iter_xml_files(in_path, args.kind))
    if args.limit:
        files = files[: args.limit]

    with engine.begin() as conn:
        for p in tqdm(files, desc="Parsing XML"):
            try:
                doc = etree.parse(str(p))
                race = parse_race_stub(doc)
                upsert_race(conn, race)
            except Exception as e:
                print(f"ERROR: {p} -> {e}")

if __name__ == "__main__":
    main()
