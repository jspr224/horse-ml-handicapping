"""
chart_xml.py — Parse Equibase-style race result chart XML files
and upsert data into staging tables in the horse_handicapping schema.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from lxml import etree
from sqlalchemy import text

from hhml.db.connect import get_engine
from hhml.db.files import register_file
from hhml.utils import fingerprint, safe_float, safe_int

# -------------------------------------------------
# Helper
# -------------------------------------------------


def first_text(node, *paths: str) -> str | None:
    """Return first non-empty text from a set of possible tag paths."""
    for p in paths:
        v = node.findtext(p)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


# -------------------------------------------------
# Core extractor
# -------------------------------------------------


def _emit_rows_chart(
    doc: etree._ElementTree, default_track: str, default_date: str
) -> dict[str, list[dict[str, Any]]]:
    root = doc.getroot()

    track_code = first_text(root, ".//Track/Code", ".//TRACK/CODE") or default_track
    rdate = first_text(root, ".//RaceDate", ".//RACE_DATE") or default_date

    race_rows, entry_rows, payout_rows, scratch_rows = [], [], [], []

    # Iterate races (case-insensitive)
    for race in root.findall(".//Race") + root.findall(".//RACE"):
        rnum = safe_int(first_text(race, "RaceNumber", "RACENUMBER", "Number", "NUMBER"))
        dist = safe_int(first_text(race, "DistanceYards", "DISTANCE_YARDS", "Distance", "DISTANCE"))
        surf = first_text(race, "Surface", "SURFACE")
        cond = first_text(race, "TrackCondition", "TRACK_CONDITION", "Condition", "CONDITION")
        # purse = safe_float(first_text(race, "Purse", "PURSE"))
        wtime = first_text(race, "WinningTime", "WINNING_TIME", "FinalTime", "FINAL_TIME")

        rr = {
            "track_code": track_code,
            "race_date": rdate,
            "race_number": rnum,
            "surface": surf,
            "distance_yards": dist,
            "track_condition": cond,
            # "purse": purse,
            "winning_time": wtime,
        }
        rr["row_fingerprint"] = fingerprint(rr)
        race_rows.append(rr)

        # Entries (results)
        for e in race.findall(".//Starter") + race.findall(".//STARTER"):
            prog = first_text(
                e, "Program", "PROGRAM", "PostPosition", "POST_POSITION", "Number", "NUMBER"
            )
            horse = first_text(e, "HorseName", "HORSE_NAME")
            finish_pos = safe_int(
                first_text(e, "FinishPosition", "FINISH_POSITION", "Finish", "FINISH")
            )
            final_odds = first_text(e, "Odds", "ODDS", "FinalOdds", "FINAL_ODDS")

            win_pay = safe_float(first_text(e, "WinPayoff", "WIN_PAYOFF"))
            plc_pay = safe_float(first_text(e, "PlacePayoff", "PLACE_PAYOFF"))
            shw_pay = safe_float(first_text(e, "ShowPayoff", "SHOW_PAYOFF"))

            er = {
                "track_code": track_code,
                "race_date": rdate,
                "race_number": rnum,
                "program_number": prog,
                "horse_name": horse,
                "finish_position": finish_pos,
                "final_odds": final_odds,
                "win_payoff": win_pay,
                "place_payoff": plc_pay,
                "show_payoff": shw_pay,
            }
            er["row_fingerprint"] = fingerprint(er)
            entry_rows.append(er)

        # Exotic payouts (optional)
        for p in race.findall(".//Payout") + race.findall(".//PAYOUT"):
            wager_type = first_text(p, "WagerType", "WAGER_TYPE", "Type", "TYPE")
            winning_nums = first_text(p, "WinningNumbers", "WINNING_NUMBERS")
            pool = safe_float(first_text(p, "Pool", "POOL"))
            payout = safe_float(first_text(p, "Payout", "PAYOUT"))

            pr = {
                "track_code": track_code,
                "race_date": rdate,
                "race_number": rnum,
                "wager_type": wager_type,
                "winning_numbers": winning_nums,
                "pool": pool,
                "payout_amount": payout,
            }
            pr["row_fingerprint"] = fingerprint(pr)
            payout_rows.append(pr)

        # Scratches
        for s in race.findall(".//Scratch") + race.findall(".//SCRATCH"):
            prog = first_text(s, "Program", "PROGRAM")
            horse = first_text(s, "HorseName", "HORSE_NAME")
            reason = first_text(s, "Reason", "REASON")

            sr = {
                "track_code": track_code,
                "race_date": rdate,
                "race_number": rnum,
                "program_number": prog,
                "horse_name": horse,
                "reason": reason,
            }
            sr["row_fingerprint"] = fingerprint(sr)
            scratch_rows.append(sr)

    return {
        "race": race_rows,
        "entry": entry_rows,
        "payout": payout_rows,
        "scratch": scratch_rows,
    }


# -------------------------------------------------
# Upsert staging
# -------------------------------------------------


def _upsert_staging(engine, file_id: int, rows: dict[str, list[dict[str, Any]]]) -> None:
    with engine.begin() as conn:
        if rows["race"]:
            conn.execute(
                text(
                    """
                    insert into horse_handicapping.stg_chart_race
                    (source_file_id, track_code, race_date, race_number,
                     surface, distance_yards, track_condition,
                     winning_time, row_fingerprint)
                    values
                    (:fid, :track_code, :race_date, :race_number,
                     :surface, :distance_yards, :track_condition, 
                     :winning_time, :row_fingerprint)
                    on conflict (source_file_id, track_code, race_date, race_number)
                    do update set
                      surface = excluded.surface,
                      distance_yards = excluded.distance_yards,
                      track_condition = excluded.track_condition,
                      winning_time = excluded.winning_time,
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


# -------------------------------------------------
# CLI entrypoint
# -------------------------------------------------


def main() -> None:
    ap = argparse.ArgumentParser(description="Parse Equibase Chart XML to staging tables")
    ap.add_argument("xml_path", type=str, help="Path to chart XML file")
    ap.add_argument("--echo", action="store_true", help="Echo SQL execution")
    args = ap.parse_args()

    p = Path(args.xml_path).expanduser().resolve()
    if not p.exists():
        raise SystemExit(f"File not found: {p}")

    engine = get_engine(echo=args.echo)

    from hhml.ingest.pp_xml import _defaults_from_filename

    d_track, d_date = _defaults_from_filename(p)

    doc = etree.parse(str(p))

    file_id = register_file(
        engine,
        p,  # Path to the XML you’re parsing
        d_track,  # default track from filename or parsed
        d_date,  # default date from filename or parsed
        provider="equibase",
        file_type="chart",
    )

    rows = _emit_rows_chart(doc, d_track, d_date)
    _upsert_staging(engine, file_id, rows)


if __name__ == "__main__":
    main()
