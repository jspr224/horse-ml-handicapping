# src/hhml/ingest/chart_xml.py
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from lxml import etree
from sqlalchemy import text

from hhml.db.connect import get_engine
from hhml.db.files import register_file
from hhml.utils import fingerprint, first_text, safe_float, safe_int

# -----------------------------
# Helpers
# -----------------------------


def _defaults_from_filename(p: Path) -> tuple[str | None, str | None]:
    """
    Infer (track_code, race_date) from filenames like:
      kee20231014tch.xml  -> (KEE, 2023-10-14)
      KEE20231014TCH.XML  -> (KEE, 2023-10-14)
    If pattern doesn't match, return (None, None).
    """
    stem = p.name
    try:
        s = stem.lower()
        # expect: <track:3 letters><yyyymmdd>tch
        # find the date block:
        import re

        m = re.search(r"([a-z]{3})?(\d{8})tch", s)
        if not m:
            return None, None
        track = (m.group(1) or "").upper() if m.group(1) else None
        ymd = m.group(2)
        rdate = f"{ymd[0:4]}-{ymd[4:6]}-{ymd[6:8]}"
        return track, rdate
    except Exception:
        return None, None


def _yards_from_distance(dist_raw: str | None, dist_unit: str | None) -> int | None:
    """
    Equibase Chart XML encodes DISTANCE with a companion DIST_UNIT.
    In your sample, DIST_UNIT is 'F' (furlongs) and DISTANCE is like 650 = 6.5f.
    Convert to yards:
      Furlong -> 220 yards
      Mile    -> 1760 yards
      Yard    -> 1 yard
    Unknown -> None
    """
    d = safe_float(dist_raw)
    if d is None:
        return None
    unit = (dist_unit or "").strip().upper()
    # In TCH files, distance is coded in hundredths of a unit (e.g., 650 -> 6.50 furlongs)
    val_units = d / 100.0

    if unit == "F":  # furlongs
        yards = val_units * 220.0
    elif unit == "M":  # miles
        yards = val_units * 1760.0
    elif unit == "Y":  # yards
        yards = val_units  # already yards
    else:
        return None

    return int(round(yards))


def _emit_rows_chart(
    doc: etree._ElementTree, default_track: str | None, default_date: str | None
) -> dict[str, list[dict[str, Any]]]:
    """
    Parse Equibase TCH (charts) XML into staging row dicts.
    Produces keys: race, entry, payout, scratch
    """
    root = doc.getroot()

    race_rows: list[dict[str, Any]] = []
    entry_rows: list[dict[str, Any]] = []
    payout_rows: list[dict[str, Any]] = []
    scratch_rows: list[dict[str, Any]] = []

    # Track/date from filename (TCH files usually lack a single header code)
    track = default_track
    rdate = default_date

    # Iterate races
    for r in root.findall(".//RACE"):
        rnum = safe_int(r.get("NUMBER")) or safe_int(first_text(r, "RACE_NUMBER"))
        surface = first_text(r, "SURFACE")  # e.g., 'D', 'T'
        trk_cond = first_text(r, "TRK_COND")  # e.g., 'FT', 'GD', ...
        dist_raw = first_text(r, "DISTANCE")
        dist_unit = first_text(r, "DIST_UNIT")
        distance_yards = _yards_from_distance(dist_raw, dist_unit)

        rrow = {
            "track_code": track,
            "race_date": rdate,
            "race_number": rnum,
            "surface": surface,
            "distance_yards": distance_yards,
            "track_condition": trk_cond,
        }
        rrow["row_fingerprint"] = fingerprint(rrow)
        race_rows.append(rrow)

        # --- Entries (finishers) ---
        for e in r.findall("./ENTRY"):
            prog = first_text(e, "PROGRAM_NUM")  # program number like "1", "1A"
            horse = first_text(e, "NAME")
            finish = safe_int(first_text(e, "OFFICIAL_FIN"))
            odds = safe_float(first_text(e, "DOLLAR_ODDS"))
            win_pay = safe_float(first_text(e, "WIN_PAYOFF"))
            pl_pay = safe_float(first_text(e, "PLACE_PAYOFF"))
            sh_pay = safe_float(first_text(e, "SHOW_PAYOFF"))

            # Only emit if we have a program number and a horse name
            if not prog or not horse:
                continue

            erow = {
                "track_code": track,
                "race_date": rdate,
                "race_number": rnum,
                "program_number": prog.strip(),
                "horse_name": horse.strip(),
                "finish_position": finish,
                "final_odds": odds,
                "win_payoff": win_pay,
                "place_payoff": pl_pay,
                "show_payoff": sh_pay,
            }
            erow["row_fingerprint"] = fingerprint(erow)
            entry_rows.append(erow)

        # --- Exotics / payouts ---
        wagers_parent = r.find("./EXOTIC_WAGERS")
        if wagers_parent is not None:
            for w in wagers_parent.findall("./WAGER"):
                wtype = first_text(w, "WAGER_TYPE")  # e.g., Exacta, Trifecta, Pick 5
                winners = first_text(w, "WINNERS")  # string like " 3-8-5"
                pool = safe_float(first_text(w, "POOL_TOTAL"))
                payoff = safe_float(first_text(w, "PAYOFF"))

                if not wtype:
                    continue

                prow = {
                    "track_code": track,
                    "race_date": rdate,
                    "race_number": rnum,
                    "wager_type": wtype.strip(),
                    "winning_numbers": winners.strip() if winners else None,
                    "pool": pool,
                    "payout_amount": payoff,
                }
                prow["row_fingerprint"] = fingerprint(prow)
                payout_rows.append(prow)

        # --- Scratches ---
        for s in r.findall("./SCRATCH"):
            horse = first_text(s, "NAME")
            reason = first_text(s, "REASON")
            # PROGRAM_NUM often not present in SCRATCH for TCH; keep None if absent
            prog = first_text(s, "PROGRAM_NUM")

            if not horse:
                continue

            srow = {
                "track_code": track,
                "race_date": rdate,
                "race_number": rnum,
                "program_number": prog.strip() if prog else None,
                "horse_name": horse.strip(),
                "reason": reason.strip() if reason else None,
            }
            srow["row_fingerprint"] = fingerprint(srow)
            scratch_rows.append(srow)

    return {"race": race_rows, "entry": entry_rows, "payout": payout_rows, "scratch": scratch_rows}


# -----------------------------
# Upsert staging tables
# -----------------------------


def _upsert_staging(engine, file_id: int, rows: dict[str, list[dict[str, Any]]]) -> None:
    with engine.begin() as conn:
        if rows["race"]:
            conn.execute(
                text(
                    """
                    insert into horse_handicapping.stg_chart_race
                    (source_file_id, track_code, race_date, race_number,
                     surface, distance_yards, track_condition,
                     row_fingerprint)
                    values
                    (:fid, :track_code, :race_date, :race_number,
                     :surface, :distance_yards, :track_condition,
                     :row_fingerprint)
                    on conflict (source_file_id, track_code, race_date, race_number)
                    do update set
                      surface = excluded.surface,
                      distance_yards = excluded.distance_yards,
                      track_condition = excluded.track_condition,
                      row_fingerprint = excluded.row_fingerprint
                """
                ),
                [dict(fid=file_id, **r) for r in rows["race"]],
            )

        if rows["entry"]:
            conn.execute(
                text(
                    """
                    insert into horse_handicapping.stg_chart_entry
                    (source_file_id, track_code, race_date, race_number,
                     program_number, horse_name, finish_position, final_odds,
                     win_payoff, place_payoff, show_payoff, row_fingerprint)
                    values
                    (:fid, :track_code, :race_date, :race_number,
                     :program_number, :horse_name, :finish_position, :final_odds,
                     :win_payoff, :place_payoff, :show_payoff, :row_fingerprint)
                    on conflict (source_file_id, track_code, race_date, race_number, program_number)
                    do update set
                      finish_position = excluded.finish_position,
                      final_odds = excluded.final_odds,
                      win_payoff = excluded.win_payoff,
                      place_payoff = excluded.place_payoff,
                      show_payoff = excluded.show_payoff,
                      row_fingerprint = excluded.row_fingerprint
                """
                ),
                [dict(fid=file_id, **r) for r in rows["entry"]],
            )

        if rows["payout"]:
            conn.execute(
                text(
                    """
                    insert into horse_handicapping.stg_chart_payout
                    (source_file_id, track_code, race_date, race_number,
                     wager_type, winning_numbers, pool, payout_amount, row_fingerprint)
                    values
                    (:fid, :track_code, :race_date, :race_number,
                     :wager_type, :winning_numbers, :pool, :payout_amount, :row_fingerprint)
                    on conflict (source_file_id, track_code, race_date, race_number, wager_type)
                    do update set
                      winning_numbers = excluded.winning_numbers,
                      pool = excluded.pool,
                      payout_amount = excluded.payout_amount,
                      row_fingerprint = excluded.row_fingerprint
                """
                ),
                [dict(fid=file_id, **r) for r in rows["payout"]],
            )

        if rows["scratch"]:
            conn.execute(
                text(
                    """
                    insert into horse_handicapping.stg_chart_scratch
                    (source_file_id, track_code, race_date, race_number,
                     program_number, horse_name, reason, row_fingerprint)
                    values
                    (:fid, :track_code, :race_date, :race_number,
                     :program_number, :horse_name, :reason, :row_fingerprint)
                    on conflict (source_file_id, track_code, race_date, race_number, program_number)
                    do update set
                      horse_name = excluded.horse_name,
                      reason = excluded.reason,
                      row_fingerprint = excluded.row_fingerprint
                """
                ),
                [dict(fid=file_id, **r) for r in rows["scratch"]],
            )


# -----------------------------
# CLI
# -----------------------------


def main() -> None:
    ap = argparse.ArgumentParser(description="Parse Equibase Chart (TCH) XML to staging")
    ap.add_argument("xml_path", type=str, help="Path to TCH XML file")
    ap.add_argument("--echo", action="store_true", help="Echo SQL (via engine echo)")
    args = ap.parse_args()

    p = Path(args.xml_path).expanduser().resolve()
    if not p.exists():
        raise SystemExit(f"File not found: {p}")

    # Engine
    engine = get_engine(echo=args.echo)

    # Defaults from filename
    d_track, d_date = _defaults_from_filename(p)

    # Parse XML
    doc = etree.parse(str(p))

    # Register file and extract rows
    file_id = register_file(engine, p, d_track, d_date, provider="equibase", file_type="chart")
    rows = _emit_rows_chart(doc, default_track=d_track, default_date=d_date)

    # Upsert staging
    _upsert_staging(engine, file_id, rows)


if __name__ == "__main__":
    main()
