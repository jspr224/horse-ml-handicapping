from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import Any

from lxml import etree
from sqlalchemy import text
from sqlalchemy.engine import Engine

from hhml.db.connect import get_engine
from hhml.ingest.utils import fingerprint, safe_float, safe_int


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def register_file(engine: Engine, p: Path, track_code: str | None, race_date: str | None) -> int:
    """Register file in raw_ingest_file; idempotent on file_hash."""
    file_hash = _sha256_file(p)
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                insert into horse_handicapping.raw_ingest_file
                  (provider, file_type, track_code, race_date, file_name, file_hash)
                values ('equibase','pp', :track_code, :race_date, :file_name, :file_hash)
                on conflict (file_hash) do update
                  set file_name = excluded.file_name
                returning file_id
                """
            ),
            {
                "track_code": track_code,
                "race_date": race_date,
                "file_name": p.name,
                "file_hash": file_hash,
            },
        )
        return res.scalar_one()


def _emit_rows_pp(
    doc: etree._ElementTree,
    default_track: str | None = None,
    default_date: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    root = doc.getroot()

    # Helpers
    def first_text(node, *paths: str) -> str | None:
        for p in paths:
            v = node.findtext(p)
            if v is not None and str(v).strip() != "":
                return str(v).strip()
        return None

    # File-level metadata: prefer filename-derived defaults if XML is sparse
    track = (first_text(root, ".//Track/Code", ".//TRACK/CODE") or default_track or "UNK").strip()
    rdate = (first_text(root, ".//RaceDate", ".//RACE_DATE") or default_date or "").strip()

    race_rows: list[dict[str, Any]] = []
    entry_rows: list[dict[str, Any]] = []
    work_rows: list[dict[str, Any]] = []

    # Races (support mixed casing / attributes)
    for race in root.findall(".//Race") + root.findall(".//RACE"):
        # robust race number extraction
        rnum_txt = (
            first_text(race, "Number", "NUMBER", "RaceNumber", "RACE_NUMBER")
            or race.get("Number")
            or race.get("NUMBER")
            or race.get("num")
        )
        rnum = safe_int(rnum_txt)
        if rnum is None:
            # Skip malformed blocks to avoid NOT NULL failures downstream
            continue

        surface = first_text(race, "Surface", "SURFACE")
        distance_yd = safe_int(first_text(race, "DistanceYards", "DISTANCE_YARDS", "DistanceYd"))
        condition = first_text(race, "TrackCondition", "TRACK_CONDITION", "Condition")
        age_restr = first_text(race, "AgeRestriction", "AGE_RESTRICTION")
        sex_restr = first_text(race, "SexRestriction", "SEX_RESTRICTION")
        purse = safe_int(first_text(race, "Purse", "PURSE"))
        wager_text = first_text(race, "WagerText", "WAGER_TEXT")
        prog_sel = first_text(race, "ProgramSelections", "PROGRAM_SELECTIONS")

        row = {
            "track_code": track,
            "race_date": rdate,
            "race_number": rnum,
            "surface": surface,
            "distance_yards": distance_yd,
            "course": None,
            "track_condition": condition,
            "age_restriction": age_restr,
            "sex_restriction": sex_restr,
            "purse": purse,
            "wager_text": wager_text,
            "program_selections": prog_sel,
        }
        row["row_fingerprint"] = fingerprint(row)
        race_rows.append(row)

        # Entries
        for e in race.findall(".//Starter") + race.findall(".//STARTER"):

            def et(node, *paths):
                return first_text(node, *paths)

            prog = et(e, "Program", "PROGRAM")
            horse = et(e, "HorseName", "HORSE_NAME")
            sire = et(e, "Sire", "SIRE")
            dam = et(e, "Dam", "DAM")
            trainer = et(e, "TrainerName", "TRAINER_NAME")
            jockey = et(e, "JockeyName", "JOCKEY_NAME")

            med = (et(e, "Medication", "MEDICATION") or "").upper()
            eqp = (et(e, "Equipment", "EQUIPMENT") or "").upper()
            lasix = "LASIX" in med if med else None
            blinkers = "BLINK" in eqp if eqp else None

            ml_odds = et(e, "MorningLine", "MORNING_LINE")
            spd = safe_int(et(e, "SpeedFigure", "SPEED_FIGURE"))
            pf1 = safe_int(et(e, "PaceFigure1", "PACE_FIGURE1"))
            pf2 = safe_int(et(e, "PaceFigure2", "PACE_FIGURE2"))
            pf3 = safe_int(et(e, "PaceFigure3", "PACE_FIGURE3"))
            cr = safe_int(et(e, "ClassRating", "CLASS_RATING"))
            cmt = et(e, "ShortComment", "LONG_COMMENT", "COMMENT")

            erow = {
                "track_code": track,
                "race_date": rdate,
                "race_number": rnum,
                "program_number": prog,
                "horse_name": horse,
                "sire": sire,
                "dam": dam,
                "trainer_name": trainer,
                "jockey_name": jockey,
                "med_lasix": lasix,
                "equip_blinkers": blinkers,
                "ml_odds": ml_odds,
                "speed_fig_last": spd,
                "pace_fig1": pf1,
                "pace_fig2": pf2,
                "pace_fig3": pf3,
                "class_rating": cr,
                "last_comment": cmt,
            }
            erow["row_fingerprint"] = fingerprint(erow)
            entry_rows.append(erow)

    # Workouts may be outside Race nodes
    for w in root.findall(".//Workout") + root.findall(".//WORKOUT"):

        def wt(node, *paths):
            return first_text(node, *paths)

        wrow = {
            "horse_name": wt(w, "HorseName", "HORSE_NAME"),
            "work_date": wt(w, "Date", "DATE"),
            "track_code": wt(w, "Track", "TRACK") or track,
            "distance_furlongs": safe_float(wt(w, "DistanceFurlongs", "DIST_FURLONGS")),
            "surface": wt(w, "Surface", "SURFACE"),
            "course_type": wt(w, "CourseType", "COURSE_TYPE"),
            "rank_in_set": safe_int(wt(w, "Rank", "RANK")),
            "set_size": safe_int(wt(w, "SetSize", "SET_SIZE")),
            "time_raw": wt(w, "Time", "TIME"),
            "bullet_flag": (wt(w, "Bullet", "BULLET") or "").upper() in ("Y", "TRUE", "1"),
        }
        wrow["row_fingerprint"] = fingerprint(wrow)
        work_rows.append(wrow)

    return {"race": race_rows, "entry": entry_rows, "workout": work_rows}


def _upsert_staging(engine: Engine, file_id: int, rows: dict[str, Any]) -> None:
    with engine.begin() as conn:
        if rows["race"]:
            conn.execute(
                text(
                    """
                    insert into horse_handicapping.stg_pp_race
                    (source_file_id, track_code, race_date, race_number, surface,
                     distance_yards, course, track_condition, age_restriction,
                     sex_restriction, purse, wager_text, program_selections,
                     row_fingerprint)
                    values
                    (:fid, :track_code, :race_date, :race_number, :surface,
                     :distance_yards, :course, :track_condition, :age_restriction,
                     :sex_restriction, :purse, :wager_text, :program_selections,
                     :row_fingerprint)
                    on conflict (source_file_id, track_code, race_date, race_number)
                    do update set
                      surface = excluded.surface,
                      distance_yards = excluded.distance_yards,
                      track_condition = excluded.track_condition,
                      purse = excluded.purse,
                      wager_text = excluded.wager_text,
                      program_selections = excluded.program_selections,
                      row_fingerprint = excluded.row_fingerprint
                    """
                ),
                [{"fid": file_id, **r} for r in rows["race"]],
            )

        if rows["entry"]:
            conn.execute(
                text(
                    """
                    insert into horse_handicapping.stg_pp_entry
                    (source_file_id, track_code, race_date, race_number,
                     program_number, horse_name, sire, dam, trainer_name,
                     jockey_name, med_lasix, equip_blinkers, ml_odds,
                     speed_fig_last, pace_fig1, pace_fig2, pace_fig3,
                     class_rating, last_comment, row_fingerprint)
                    values
                    (:fid, :track_code, :race_date, :race_number, :program_number,
                     :horse_name, :sire, :dam, :trainer_name, :jockey_name,
                     :med_lasix, :equip_blinkers, :ml_odds, :speed_fig_last,
                     :pace_fig1, :pace_fig2, :pace_fig3, :class_rating,
                     :last_comment, :row_fingerprint)
                    on conflict
                    (source_file_id, track_code, race_date, race_number, program_number)
                    do update set
                      horse_name = excluded.horse_name,
                      trainer_name = excluded.trainer_name,
                      jockey_name = excluded.jockey_name,
                      ml_odds = excluded.ml_odds,
                      speed_fig_last = excluded.speed_fig_last,
                      pace_fig1 = excluded.pace_fig1,
                      pace_fig2 = excluded.pace_fig2,
                      pace_fig3 = excluded.pace_fig3,
                      class_rating = excluded.class_rating,
                      last_comment = excluded.last_comment,
                      row_fingerprint = excluded.row_fingerprint
                    """
                ),
                [{"fid": file_id, **e} for e in rows["entry"]],
            )

        if rows["workout"]:
            conn.execute(
                text(
                    """
                    insert into horse_handicapping.stg_pp_workout
                    (source_file_id, horse_name, work_date, track_code,
                     distance_furlongs, surface, course_type, rank_in_set,
                     set_size, time_raw, bullet_flag, row_fingerprint)
                    values
                    (:fid, :horse_name, :work_date, :track_code, :distance_furlongs,
                     :surface, :course_type, :rank_in_set, :set_size, :time_raw,
                     :bullet_flag, :row_fingerprint)
                    on conflict
                    (source_file_id, horse_name, work_date, track_code, distance_furlongs)
                    do update set
                      time_raw = excluded.time_raw,
                      bullet_flag = excluded.bullet_flag,
                      row_fingerprint = excluded.row_fingerprint
                    """
                ),
                [{"fid": file_id, **w} for w in rows["workout"]],
            )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("xml", nargs="+", help="One or more SIMD*.xml PP files")
    ap.add_argument("--track", help="Override track code", required=False)
    ap.add_argument("--date", help="Override race date YYYY-MM-DD", required=False)
    ap.add_argument("--echo", action="store_true")
    args = ap.parse_args()

    engine = get_engine(echo=args.echo)

    for x in args.xml:
        p = Path(x)
        doc = etree.parse(str(p))

        # Derive track/date from filename if missing (SIMDYYYYMMDDTRK_*.xml)
        track = args.track
        rdate = args.date
        stem = p.stem
        if (not track or not rdate) and stem.startswith("SIMD") and len(stem) >= 15:
            yyyymmdd = stem[4:12]
            trk = stem[12:15]
            rdate = rdate or f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:]}"
            track = track or trk

        file_id = register_file(engine, p, track, rdate)
        rows = _emit_rows_pp(doc, default_track=track, default_date=rdate)
        _upsert_staging(engine, file_id, rows)


if __name__ == "__main__":
    main()
