# src/hhml/ingest/chart_xml.py
from __future__ import annotations

import argparse
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from lxml import etree
from sqlalchemy import text

from hhml.db.connect import get_engine
from hhml.db.files import register_file
from hhml.utils import fingerprint, safe_float, safe_int

SCHEMA = "horse_handicapping"


# -----------------------------
# XML helpers (namespace-agnostic)
# -----------------------------
def first_text(node: etree._Element, *names: str) -> str | None:
    for nm in names:
        hits = node.xpath(f"./*[local-name()='{nm}']/text()")
        if hits:
            v = str(hits[0]).strip()
            if v != "":
                return v
    return None


def findall(node: etree._Element, *names: str) -> list[etree._Element]:
    out: list[etree._Element] = []
    for nm in names:
        out.extend(node.xpath(f".//*[local-name()='{nm}']"))
    return out


# -----------------------------
# Distance/SURFACE helpers
# -----------------------------
def yards_from_furlongs_miles(dist_str: str | None) -> int | None:
    """
    Convert chart-style distance strings to yards.
    Examples: "6 Furlongs" -> 1320, "1 Mile" -> 1760, "1 1/16 Miles" -> 1870
    """
    if not dist_str:
        return None
    s = dist_str.lower().strip()
    # Furlongs simple
    if "furlong" in s:
        import re

        m = re.search(r"(\d+(?:\.\d+)?)", s)
        if not m:
            return None
        furlongs = float(m.group(1))
        return int(round(furlongs * 220))

    # Miles with optional fractions
    if "mile" in s:
        # Examples: "1 Mile", "1 1/16 Miles", "1 1/8 Miles", "1 3/8 Miles"
        import re

        parts = s.split("mile")[0].strip()
        # mixed fraction like "1 1/16"
        m_mixed = re.match(r"^\s*(\d+)\s+(\d+)/(\d+)", parts)
        if m_mixed:
            whole = float(m_mixed.group(1))
            num = float(m_mixed.group(2))
            den = float(m_mixed.group(3))
            miles = whole + (num / den)
        else:
            m_simple = re.match(r"^\s*(\d+(?:\.\d+)?)", parts)
            if not m_simple:
                return None
            miles = float(m_simple.group(1))
        return int(round(miles * 1760))

    return None


def surface_code(val: str | None) -> str | None:
    if not val:
        return None
    v = val.strip().upper()
    if v.startswith("TURF"):
        return "T"
    if v.startswith("DIRT"):
        return "D"
    if v.startswith("ALL WEATHER") or v.startswith("SYN"):
        return "A"
    return v[:1] if v else None


def text_or_attr_distance(race: etree._Element) -> str | None:
    """
    Return a human-readable distance string like '7 Furlongs' or '1 1/16 Miles'
    from either text nodes or <Distance unit="...">N</Distance> forms.
    """
    # common description nodes
    desc = first_text(
        race,
        "DistanceDescription",
        "DISTANCE_DESCRIPTION",
        "DistanceDesc",
        "DIST_DESC",
    )
    if desc:
        return desc

    # sometimes it's just 'Distance'
    raw = first_text(race, "Distance", "DISTANCE")
    if raw:
        # could already be like "7 Furlongs"
        if any(w in raw.lower() for w in ("furlong", "mile")):
            return raw
        # or numeric value with a sibling/attr unit
        for el in race.xpath(".//*[local-name()='Distance']"):
            unit = (el.get("unit") or el.get("UNIT") or "").strip()
            if str(el.text or "").strip() == raw.strip() and unit:
                unit_norm = unit.lower()
                if unit_norm.startswith("fur"):
                    return f"{raw} Furlongs"
                if unit_norm.startswith("mil"):
                    try:
                        v = float(raw)
                        if abs(v - int(v)) < 1e-9:
                            return f"{int(v)} Mile" if int(v) == 1 else f"{int(v)} Miles"
                    except Exception:
                        pass
                    return f"{raw} Miles"
        # last resort: assume furlongs
        return f"{raw} Furlongs"

    return None


# -----------------------------
# Row containers
# -----------------------------
@dataclass
class RaceRow:
    track_code: str | None
    race_date: str | None
    race_number: int | None
    surface: str | None
    distance_yards: int | None
    track_condition: str | None


@dataclass
class EntryRow:
    track_code: str
    race_date: str
    race_number: int
    program_number: str
    horse_name: str
    finish_position: int | None
    final_odds: float | None
    win_payoff: float | None
    place_payoff: float | None
    show_payoff: float | None


# -----------------------------
# Emit rows from XML (Equibase TCH)
# -----------------------------
def emit_rows_chart(
    doc: etree._ElementTree,
    default_track: str | None,
    default_date: str | None,
) -> dict[str, list[dict[str, Any]]]:
    root = doc.getroot()

    # Track and date at meet level if present; fall back to filename defaults
    tcode = first_text(root, "TrackCode", "TRACK_CODE") or default_track
    rdate = first_text(root, "RaceDate", "RACE_DATE") or default_date

    race_rows: list[dict[str, Any]] = []
    entry_rows: list[dict[str, Any]] = []

    race_nodes = findall(root, "Race", "RACE")

    for idx, race in enumerate(race_nodes, start=1):
        rnum = safe_int(
            first_text(
                race,
                # common variants
                "RaceNumber",
                "RACE_NUMBER",
                "RaceNum",
                "RACE_NUM",
                "Number",
                "NUMBER",
                "RaceNo",
                "RACE_NO",
            )
        )
        if rnum is None:
            # last resort: use ordinal position in file
            rnum = idx

        # ---- distance / condition ----
        dist = text_or_attr_distance(race)
        dist_yards = yards_from_furlongs_miles(dist)
        # Fix over-scaled numeric-only distances like '154000'
        if dist_yards and dist_yards > 10000:
            dist_yards = dist_yards / 100

        cond = (
            first_text(race, "TrackCondition", "TRACK_CONDITION")
            or first_text(race, "TrackConditionCode", "TRACK_CONDITION_CODE")
            or first_text(race, "TrackConditionDesc", "TRACK_CONDITION_DESC")
        )

        surf = surface_code(first_text(race, "Surface", "SURFACE"))

        rr: dict[str, Any] = {
            "track_code": tcode,
            "race_date": rdate,
            "race_number": rnum,
            "surface": surf,
            "distance_yards": dist_yards,
            "track_condition": cond,
        }
        rr["row_fingerprint"] = fingerprint(rr)
        race_rows.append(rr)

        # ---- entries / starters ----
        # Parents that might contain the list of starters/entries
        parent_candidates = findall(
            race,
            "Starters",
            "STARTERS",
            "Entries",
            "ENTRIES",
            "Results",
            "RESULTS",
            "HorseList",
            "HORSELIST",
        )
        starter_nodes: list[etree._Element] = []
        for parent in parent_candidates:
            starter_nodes.extend(
                findall(parent, "Starter", "STARTER", "Entry", "ENTRY", "Horse", "HORSE")
            )

        for s in starter_nodes:
            prog = first_text(
                s,
                "ProgramNumber",
                "PROGRAM_NUMBER",
                "Program",
                "PROGRAM",
                "PostPosition",
                "POST_POSITION",
                "PP",
            )
            if prog:
                import re

                m = re.match(r"^\s*(\d{1,2})([A-C]?)\s*$", prog.strip())
                if not m:
                    continue
                prog = m.group(0).strip()
            else:
                continue

            horse = first_text(s, "Horse", "HORSE", "HorseName", "HORSE_NAME", "Name", "NAME") or ""
            finish = safe_int(
                first_text(s, "FinishPosition", "FINISH_POSITION", "Finish", "FINISH", "Pos", "POS")
            )
            odds = safe_float(first_text(s, "FinalOdds", "FINAL_ODDS", "Odds", "ODDS"))

            win_pay = safe_float(first_text(s, "WinPayoff", "WIN_PAYOFF", "WIN"))
            place_pay = safe_float(first_text(s, "PlacePayoff", "PLACE_PAYOFF", "PLACE"))
            show_pay = safe_float(first_text(s, "ShowPayoff", "SHOW_PAYOFF", "SHOW"))

            er: dict[str, Any] = {
                "track_code": tcode or "",
                "race_date": rdate or "",
                "race_number": rnum or 0,
                "program_number": prog,
                "horse_name": horse,
                "finish_position": finish,
                "final_odds": odds,
                "win_payoff": win_pay,
                "place_payoff": place_pay,
                "show_payoff": show_pay,
            }
            er["row_fingerprint"] = fingerprint(er)
            entry_rows.append(er)

        # Entries/Starters
        starters_parent = findall(race, "Starters", "STARTERS")
        starter_nodes: list[etree._Element] = []
        for sp in starters_parent:
            starter_nodes.extend(findall(sp, "Starter", "STARTER"))

        for s in starter_nodes:
            prog = first_text(
                s,
                "ProgramNumber",
                "PROGRAM_NUMBER",
                "Program",
                "PROGRAM",
                "PostPosition",
                "POST_POSITION",
            )
            if prog:
                import re

                m = re.match(r"^\s*(\d{1,2})([A-C]?)\s*$", prog.strip())
                if not m:
                    continue
                prog = m.group(0).strip()
            else:
                continue

            horse = first_text(s, "Horse", "HORSE", "HorseName", "HORSE_NAME") or ""
            finish = safe_int(
                first_text(s, "FinishPosition", "FINISH_POSITION", "Finish", "FINISH")
            )
            odds = safe_float(first_text(s, "FinalOdds", "FINAL_ODDS", "Odds", "ODDS"))

            win_pay = safe_float(first_text(s, "WinPayoff", "WIN_PAYOFF", "WIN")) or 0.0
            place_pay = safe_float(first_text(s, "PlacePayoff", "PLACE_PAYOFF", "PLACE")) or 0.0
            show_pay = safe_float(first_text(s, "ShowPayoff", "SHOW_PAYOFF", "SHOW")) or 0.0

            er: dict[str, Any] = {
                "track_code": tcode or "",
                "race_date": rdate or "",
                "race_number": rnum or 0,
                "program_number": prog,
                "horse_name": horse,
                "finish_position": finish,
                "final_odds": odds,
                "win_payoff": win_pay,
                "place_payoff": place_pay,
                "show_payoff": show_pay,
            }
            er["row_fingerprint"] = fingerprint(er)
            entry_rows.append(er)

    return {"race": race_rows, "entry": entry_rows}


# -----------------------------
# Dynamic upsert helpers
# -----------------------------
def get_table_columns(conn, table: str, schema: str = SCHEMA) -> set[str]:
    rows = conn.execute(
        text(
            """
            select column_name
            from information_schema.columns
            where table_schema = :schema and table_name = :table
            """
        ),
        {"schema": schema, "table": table},
    ).fetchall()
    return {r[0] for r in rows}


def build_upsert_sql(
    table: str, cols: Iterable[str], conflict_cols: Iterable[str], schema: str = SCHEMA
) -> str:
    cols = list(cols)
    conflict_cols = list(conflict_cols)
    bind_cols = ", ".join(f":{c}" for c in cols)
    insert_cols = ", ".join(cols)
    update_cols = [c for c in cols if c not in conflict_cols]
    update_set = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    fq = f"{schema}.{table}"
    sql = f"""
        insert into {fq}
        ({insert_cols})
        values
        ({bind_cols})
        on conflict ({", ".join(conflict_cols)})
        do update set
          {update_set}
    """
    return sql


def _upsert_staging(engine, file_id: int, rows: dict[str, list[dict[str, Any]]]) -> None:
    with engine.begin() as conn:
        race_cols_actual = get_table_columns(conn, "stg_chart_race")
        entry_cols_actual = get_table_columns(conn, "stg_chart_entry")

        race_wanted = [
            "source_file_id",
            "track_code",
            "race_date",
            "race_number",
            "surface",
            "distance_yards",
            "track_condition",
            "row_fingerprint",
        ]
        entry_wanted = [
            "source_file_id",
            "track_code",
            "race_date",
            "race_number",
            "program_number",
            "horse_name",
            "finish_position",
            "final_odds",
            "win_payoff",
            "place_payoff",
            "show_payoff",
            "row_fingerprint",
        ]

        race_pk = [
            c
            for c in ("source_file_id", "track_code", "race_date", "race_number")
            if c in race_cols_actual
        ]
        entry_pk = [
            c
            for c in ("source_file_id", "track_code", "race_date", "race_number", "program_number")
            if c in entry_cols_actual
        ]

        race_cols = [c for c in race_wanted if c in race_cols_actual]
        entry_cols = [c for c in entry_wanted if c in entry_cols_actual]

        if rows["race"] and race_cols and race_pk:
            sql = build_upsert_sql("stg_chart_race", race_cols, race_pk)
            payload = [{**r, "source_file_id": file_id} for r in rows["race"]]
            payload = [{k: v for k, v in d.items() if k in race_cols} for d in payload]
            conn.execute(text(sql), payload)

        if rows["entry"] and entry_cols and entry_pk:
            sql = build_upsert_sql("stg_chart_entry", entry_cols, entry_pk)
            payload = [{**r, "source_file_id": file_id} for r in rows["entry"]]
            payload = [{k: v for k, v in d.items() if k in entry_cols} for d in payload]
            conn.execute(text(sql), payload)


# -----------------------------
# CLI
# -----------------------------
def _defaults_from_filename(p: Path) -> tuple[str | None, str | None]:
    # kee20231014tch.xml -> track=KEE, date=2023-10-14
    stem = p.stem
    if len(stem) >= 11:
        track = stem[:3].upper()
        date_part = stem[3:11]
        if date_part.isdigit():
            y, m, d = date_part[:4], date_part[4:6], date_part[6:8]
            return track, f"{y}-{m}-{d}"
    return None, None


def main() -> None:
    ap = argparse.ArgumentParser(description="Parse Equibase Chart (TCH) XML to staging")
    ap.add_argument("xml_path", type=str, help="Path to chart XML file")
    ap.add_argument("--echo", action="store_true", help="Echo SQL (via engine echo)")
    args = ap.parse_args()

    p = Path(args.xml_path).expanduser().resolve()
    if not p.exists():
        raise SystemExit(f"File not found: {p}")

    engine = get_engine(echo=args.echo)

    d_track, d_date = _defaults_from_filename(p)
    doc = etree.parse(str(p))

    file_id = register_file(engine, p, d_track, d_date, provider="equibase", file_type="chart")
    rows = emit_rows_chart(doc, default_track=d_track, default_date=d_date)

    _upsert_staging(engine, file_id, rows)


if __name__ == "__main__":
    main()
