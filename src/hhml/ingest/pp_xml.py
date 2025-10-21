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
# XML helpers (namespace-agnostic)
# -----------------------------


def _iter_local(node: etree._Element, *names: str) -> list[etree._Element]:
    """Return descendants whose local-name() matches any provided name."""
    out: list[etree._Element] = []
    for nm in names:
        out.extend(node.xpath(f".//*[local-name()='{nm}']"))
    return out


def _first_text_local(node: etree._Element, *names: str) -> str | None:
    """First non-empty text for any descendant with local-name in names.
    Also checks attributes of those nodes for values if text is empty.
    """
    for el in _iter_local(node, *names):
        if el.text and str(el.text).strip():
            return str(el.text).strip()
        # look at attributes like <Entry Program="1A"/>
        for k, v in el.attrib.items():
            if str(v).strip():
                return str(v).strip()
    return None


def _first_attr_local(node: etree._Element, attrs: tuple[str, ...]) -> str | None:
    for k, v in node.attrib.items():
        if k in attrs and str(v).strip():
            return str(v).strip()
    return None


# -----------------------------
# XML extraction
# -----------------------------


def _emit_rows_pp(
    doc: etree._ElementTree,
    default_track: str | None = None,
    default_date: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    root = doc.getroot()

    # Track & date (prefer filename-derived)
    track = (
        _first_text_local(root, "Code")
        or _first_text_local(root, "Track")
        or default_track
        or "UNK"
    ).strip()
    rdate = (default_date or _first_text_local(root, "RaceDate") or "").strip()

    race_rows: list[dict[str, Any]] = []
    entry_rows: list[dict[str, Any]] = []
    work_rows: list[dict[str, Any]] = []

    # ---- Races (ns-agnostic) ----
    for race in _iter_local(root, "Race"):
        # race number via tag or attribute
        rnum_txt = _first_text_local(race, "Number", "RaceNumber") or _first_attr_local(
            race, ("Number", "NUMBER", "num")
        )
        rnum = safe_int(rnum_txt)
        if rnum is None:
            # skip malformed races to satisfy NOT NULL
            continue

        surface = _first_text_local(race, "Surface")
        distance_yd = safe_int(_first_text_local(race, "DistanceYards", "DistanceYd"))
        condition = _first_text_local(race, "TrackCondition", "Condition")
        age_restr = _first_text_local(race, "AgeRestriction")
        sex_restr = _first_text_local(race, "SexRestriction")
        purse = safe_int(_first_text_local(race, "Purse"))
        wager_text = _first_text_local(race, "WagerText")
        prog_sel = _first_text_local(race, "ProgramSelections")

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

        # ---- Entries (Starter/Entry/Horse/Runner) ----
        entry_nodes: list[etree._Element] = []
        entry_nodes.extend(_iter_local(race, "Starter", "Entry", "Horse", "Runner"))

        def et(node: etree._Element, *names: str) -> str | None:
            # prefer child/descendant text; if empty, check attributes
            val = _first_text_local(node, *names)
            if val is not None and str(val).strip():
                return str(val).strip()
            return None

        def get_program_number(n: etree._Element) -> str | None:
            # 1) direct descendant text
            val = _first_text_local(
                n,
                "Program",
                "Prog",
                "PostPosition",
                "PP",
                "Number",
                "ProgNum",
                "ProgramNumber",
                "SaddleCloth",
                "EntryNumber",
            )
            # 2) attributes on the entry node
            if not val:
                val = _first_attr_local(
                    n,
                    (
                        "program",
                        "PROGRAM",
                        "prog",
                        "pp",
                        "post",
                        "number",
                        "prognum",
                        "program_number",
                        "saddlecloth",
                        "entry_number",
                    ),
                )
            # 3) normalize to [1-99][A-C]?
            if not val:
                return None
            m = re.match(r"^\s*0*(\d{1,2})([A-C]?)\s*$", str(val))
            if not m:
                return None
            return f"{m.group(1)}{m.group(2)}"

        for e in entry_nodes:
            prog = get_program_number(e)
            if not prog:
                # Strict: skip if no reliable program number
                continue

            horse = et(e, "HorseName", "Name")
            sire = et(e, "Sire")
            dam = et(e, "Dam")
            trainer = et(e, "TrainerName", "Trainer")
            jockey = et(e, "JockeyName", "Jockey")

            med = (et(e, "Medication") or "").upper()
            eqp = (et(e, "Equipment") or "").upper()
            lasix = "LASIX" in med if med else None
            blinkers = "BLINK" in eqp if eqp else None

            ml_odds = et(e, "MorningLine", "ML")
            spd = safe_int(et(e, "SpeedFigure", "Speed"))
            pf1 = safe_int(et(e, "PaceFigure1", "Pace1"))
            pf2 = safe_int(et(e, "PaceFigure2", "Pace2"))
            pf3 = safe_int(et(e, "PaceFigure3", "Pace3"))
            cr = safe_int(et(e, "ClassRating", "Class"))
            cmt = et(e, "ShortComment", "LongComment", "Comment")

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
    for w in _iter_local(root, "Workout"):

        def wt(node: etree._Element, *names: str) -> str | None:
            return _first_text_local(node, *names)

        wrow = {
            "horse_name": wt(w, "HorseName"),
            "work_date": wt(w, "Date"),
            "track_code": wt(w, "Track") or track,
            "distance_furlongs": safe_float(wt(w, "DistanceFurlongs", "DistFurlongs")),
            "surface": wt(w, "Surface"),
            "course_type": wt(w, "CourseType"),
            "rank_in_set": safe_int(wt(w, "Rank")),
            "set_size": safe_int(wt(w, "SetSize")),
            "time_raw": wt(w, "Time"),
            "bullet_flag": (wt(w, "Bullet") or "").upper() in ("Y", "TRUE", "1"),
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
