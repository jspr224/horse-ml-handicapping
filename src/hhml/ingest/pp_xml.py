from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from lxml import etree  # type: ignore
from sqlalchemy import text

# Project engine helper
from hhml.db.connect import get_engine

# -----------------------------
# Utility helpers
# -----------------------------


def safe_int(x: Any) -> int | None:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        return int(float(s))
    except Exception:
        return None


def safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def fingerprint(row: dict[str, Any]) -> str:
    """Stable row fingerprint for idempotent upserts."""
    canon = json.dumps(row, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(canon.encode("utf-8")).hexdigest()


# -----------------------------
# Filename-derived defaults
# -----------------------------

_DATE_RE = re.compile(r"(20\d{2})(\d{2})(\d{2})")
_TRACK_RE = re.compile(r"([A-Z]{2,4})[_\.]")


def _defaults_from_filename(p: Path) -> tuple[str | None, str | None]:
    name = p.name

    # date like 20230422
    rdate: str | None = None
    m = _DATE_RE.search(name)
    if m:
        try:
            rdate = datetime.strptime("".join(m.groups()), "%Y%m%d").date().isoformat()
        except Exception:
            rdate = None

    # track code: try the block of caps before underscore or dot (e.g., KEE in *_KEE_USA.xml)
    track: str | None = None
    # Prefer pattern like ...YYYYMMDDKEE_USA.xml
    m2 = re.search(r"20\d{6}([A-Z]{2,4})[_\.]", name)
    if m2:
        track = m2.group(1)
    else:
        m3 = _TRACK_RE.search(name)
        if m3:
            track = m3.group(1)

    return track, rdate


# -----------------------------
# DB helpers
# -----------------------------


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def register_file(engine, path: Path, track_code: str | None, race_date: str | None) -> int:
    """Insert or update raw_ingest_file and return file_id."""
    fhash = _sha256_file(path)
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
                "file_name": path.name,
                "file_hash": fhash,
            },
        )
        fid = res.scalar_one()
    return int(fid)


# -----------------------------
# XML extraction
# -----------------------------


def _emit_rows_pp(
    doc: etree._ElementTree,
    default_track: str | None = None,
    default_date: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    root = doc.getroot()

    def first_text(node, *paths: str) -> str | None:
        for p in paths:
            v = node.findtext(p)
            if v is not None and str(v).strip() != "":
                return str(v).strip()
        return None

    track = (first_text(root, ".//Track/Code", ".//TRACK/CODE") or default_track or "UNK").strip()
    # Prefer filename date if provided, else fall back to XML
    rdate = (default_date or first_text(root, ".//RaceDate", ".//RACE_DATE") or "").strip()

    race_rows: list[dict[str, Any]] = []
    entry_rows: list[dict[str, Any]] = []
    work_rows: list[dict[str, Any]] = []

    # ---- Races ----
    for race in root.findall(".//Race") + root.findall(".//RACE"):

        def rt(node, *paths):
            for pth in paths:
                v = node.findtext(pth)
                if v is not None and str(v).strip() != "":
                    return str(v).strip()
            return None

        # race number with robust fallbacks (tags and attributes)
        rnum_txt = (
            rt(race, "Number", "NUMBER", "RaceNumber", "RACE_NUMBER")
            or race.get("Number")
            or race.get("NUMBER")
            or race.get("num")
        )
        rnum = safe_int(rnum_txt)
        if rnum is None:
            # skip malformed races to satisfy NOT NULL
            continue

        surface = rt(race, "Surface", "SURFACE")
        distance_yd = safe_int(rt(race, "DistanceYards", "DISTANCE_YARDS", "DistanceYd"))
        condition = rt(race, "TrackCondition", "TRACK_CONDITION", "Condition")
        age_restr = rt(race, "AgeRestriction", "AGE_RESTRICTION")
        sex_restr = rt(race, "SexRestriction", "SEX_RESTRICTION")
        purse = safe_int(rt(race, "Purse", "PURSE"))
        wager_text = rt(race, "WagerText", "WAGER_TEXT")
        prog_sel = rt(race, "ProgramSelections", "PROGRAM_SELECTIONS")

        rrow = {
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
        rrow["row_fingerprint"] = fingerprint(rrow)
        race_rows.append(rrow)

        # ---- Entries ----
        entry_nodes = []
        for tag in ("Starter", "STARTER", "Entry", "ENTRY", "Horse", "HORSE", "Runner", "RUNNER"):
            entry_nodes.extend(race.findall(f".//{tag}"))

        def et(node, *paths):
            # child text
            for p in paths:
                v = node.findtext(p)
                if v is not None and str(v).strip() != "":
                    return str(v).strip()
            # attribute
            for p in paths:
                if p in node.attrib and str(node.attrib[p]).strip() != "":
                    return node.attrib[p].strip()
            return None

        def get_program_number(n) -> str | None:
            val = et(
                n,
                "Program",
                "PROGRAM",
                "PostPosition",
                "POST_POSITION",
                "PP",
                "Number",
                "NUMBER",
                "ProgNum",
                "PROGNUM",
                "ProgramNumber",
                "PROGRAM_NUMBER",
                "SaddleCloth",
                "SADDLE_CLOTH",
                "EntryNumber",
                "ENTRY_NUMBER",
            )
            if val:
                return val
            # descendant sweep for common names
            try:
                for name in (
                    "Program",
                    "PROGRAM",
                    "PostPosition",
                    "POST_POSITION",
                    "PP",
                    "Number",
                    "NUMBER",
                ):
                    hits = n.xpath(f".//*[local-name()='{name}']/text()")
                    for h in hits:
                        if str(h).strip():
                            return str(h).strip()
            except Exception:
                pass
            return None

        for e in entry_nodes:
            # --- Program number (robust) ---
            # Try common node names first
            prog = first_text(
                e,
                "Program",
                "PROGRAM",
                "Prog",
                "PROG",
                "Saddle",
                "SADDLE",
                "PP",
                "POST_POSITION",
                "PostPosition",
                "POST",
                "ENTRY_NUMBER",
            )

            # Some feeds put it on the <Entry>/<Starter> node as an attribute
            if not prog:
                for attr in ("program", "PROGRAM", "pp", "PP", "post", "POST"):
                    v = e.get(attr)
                    if v and v.strip():
                        prog = v.strip()
                        break

            # Normalize: allow 1–2 digits plus optional A/B/C suffix (e.g., "1", "1A", "12B")
            if prog:
                import re

                m = re.match(r"^\s*(\d{1,2})([A-C]?)\s*$", prog)
                prog = m.group(0).strip() if m else None

            horse = et(e, "HorseName", "HORSE_NAME", "Name", "NAME")
            sire = et(e, "Sire", "SIRE")
            dam = et(e, "Dam", "DAM")
            trainer = et(e, "TrainerName", "TRAINER_NAME", "Trainer", "TRAINER")
            jockey = et(e, "JockeyName", "JOCKEY_NAME", "Jockey", "JOCKEY")

            med = (et(e, "Medication", "MEDICATION") or "").upper()
            eqp = (et(e, "Equipment", "EQUIPMENT") or "").upper()
            lasix = "LASIX" in med if med else None
            blinkers = "BLINK" in eqp if eqp else None

            ml_odds = et(e, "MorningLine", "MORNING_LINE", "ML")
            spd = safe_int(et(e, "SpeedFigure", "SPEED_FIGURE", "Speed", "SPEED"))
            pf1 = safe_int(et(e, "PaceFigure1", "PACE_FIGURE1", "Pace1", "PACE1"))
            pf2 = safe_int(et(e, "PaceFigure2", "PACE_FIGURE2", "Pace2", "PACE2"))
            pf3 = safe_int(et(e, "PaceFigure3", "PACE_FIGURE3", "Pace3", "PACE3"))
            cr = safe_int(et(e, "ClassRating", "CLASS_RATING", "Class", "CLASS"))
            cmt = et(e, "ShortComment", "LONG_COMMENT", "Comment", "COMMENT")

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

    # ---- Workouts (often outside Race) ----
    for w in root.findall(".//Workout") + root.findall(".//WORKOUT"):

        def wt(node, *paths):
            for p in paths:
                v = node.findtext(p)
                if v is not None and str(v).strip() != "":
                    return str(v).strip()
            return None

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

        if not wrow["horse_name"]:
            continue

        wrow["row_fingerprint"] = fingerprint(wrow)
        work_rows.append(wrow)

    return {"race": race_rows, "entry": entry_rows, "workout": work_rows}


# -----------------------------
# Upsert staging tables
# -----------------------------


def _upsert_staging(engine, file_id: int, rows: dict[str, list[dict[str, Any]]]) -> None:
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
                [dict(fid=file_id, **r) for r in rows["race"]],
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
                    on conflict (source_file_id, track_code, race_date, race_number, program_number)
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
                [dict(fid=file_id, **r) for r in rows["entry"]],
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
                    on conflict (source_file_id, horse_name, work_date,
                                 track_code, distance_furlongs)
                    do update set
                      time_raw = excluded.time_raw,
                      bullet_flag = excluded.bullet_flag,
                      row_fingerprint = excluded.row_fingerprint
                    """
                ),
                [dict(fid=file_id, **r) for r in rows["workout"]],
            )


# -----------------------------
# CLI
# -----------------------------


def main() -> None:
    ap = argparse.ArgumentParser(description="Parse Equibase PP XML to staging")
    ap.add_argument("xml_path", type=str, help="Path to PP XML file")
    ap.add_argument("--echo", action="store_true", help="Echo SQL (via engine echo)")
    args = ap.parse_args()

    p = Path(args.xml_path).expanduser().resolve()
    if not p.exists():
        raise SystemExit(f"File not found: {p}")

    # Engine (respect global echo if flag is set by temporarily creating)
    engine = get_engine(echo=args.echo)

    # Filename defaults
    d_track, d_date = _defaults_from_filename(p)

    # Parse XML
    doc = etree.parse(str(p))

    # Register file and extract rows
    file_id = register_file(engine, p, d_track, d_date)
    rows = _emit_rows_pp(doc, default_track=d_track, default_date=d_date)

    # Upsert staging
    _upsert_staging(engine, file_id, rows)


if __name__ == "__main__":
    main()
