-- ===============================
-- Horse ML — Ingestion Staging & Idempotent Merge
-- ===============================

create schema if not exists horse_handicapping;

create table if not exists horse_handicapping.track (
  track_id serial primary key,
  track_code text unique not null,
  track_name text,
  state text,
  timezone text
);

create table if not exists horse_handicapping.raw_ingest_file (
  file_id bigserial primary key,
  provider text not null,
  file_type text not null,
  track_code text,
  file_date date,
  file_name text not null,
  file_hash text not null unique,
  received_at timestamptz default now(),
  processed_at timestamptz
);

create table if not exists horse_handicapping.stg_pp_race (
  source_file_id bigint references horse_handicapping.raw_ingest_file(file_id),
  track_code text,
  race_date date,
  race_number int,
  surface text,
  distance_yards int,
  course text,
  track_condition text,
  age_restriction text,
  sex_restriction text,
  purse int,
  wager_text text,
  program_selections text,
  row_fingerprint text,
  primary key (source_file_id, track_code, race_date, race_number)
);

create table if not exists horse_handicapping.stg_pp_entry (
  source_file_id bigint references horse_handicapping.raw_ingest_file(file_id),
  track_code text,
  race_date date,
  race_number int,
  program_number text,
  horse_name text,
  sire text,
  dam text,
  trainer_name text,
  jockey_name text,
  med_lasix boolean,
  equip_blinkers boolean,
  ml_odds text,
  speed_fig_last int,
  pace_fig1 int,
  pace_fig2 int,
  pace_fig3 int,
  class_rating int,
  last_comment text,
  row_fingerprint text,
  primary key (source_file_id, track_code, race_date, race_number, program_number)
);

create table if not exists horse_handicapping.stg_pp_workout (
  source_file_id bigint references horse_handicapping.raw_ingest_file(file_id),
  horse_name text,
  work_date date,
  track_code text,
  distance_furlongs numeric,
  surface text,
  course_type text,
  rank_in_set int,
  set_size int,
  time_raw text,
  bullet_flag boolean,
  row_fingerprint text,
  primary key (source_file_id, horse_name, work_date, track_code, distance_furlongs)
);

create table if not exists horse_handicapping.stg_chart_race (
  source_file_id bigint references horse_handicapping.raw_ingest_file(file_id),
  track_code text,
  race_date date,
  race_number int,
  surface text,
  distance_yards int,
  track_condition text,
  field_size int,
  row_fingerprint text,
  primary key (source_file_id, track_code, race_date, race_number)
);

create table if not exists horse_handicapping.stg_chart_entry (
  source_file_id bigint references horse_handicapping.raw_ingest_file(file_id),
  track_code text,
  race_date date,
  race_number int,
  program_number text,
  horse_name text,
  finish_pos int,
  beaten_lengths numeric,
  final_time text,
  odds_final numeric,
  dq_flag boolean,
  scratch_flag boolean,
  row_fingerprint text,
  primary key (source_file_id, track_code, race_date, race_number, program_number)
);

create table if not exists horse_handicapping.stg_chart_payout (
  source_file_id bigint references horse_handicapping.raw_ingest_file(file_id),
  track_code text,
  race_date date,
  race_number int,
  pool_type text,
  base_amount numeric,
  combination text,
  payout numeric,
  pool_total numeric,
  row_fingerprint text,
  primary key (source_file_id, track_code, race_date, race_number, pool_type, combination)
);

create table if not exists horse_handicapping.stg_chart_scratch (
  source_file_id bigint references horse_handicapping.raw_ingest_file(file_id),
  track_code text,
  race_date date,
  race_number int,
  program_number text,
  reason text,
  row_fingerprint text,
  primary key (source_file_id, track_code, race_date, race_number, program_number)
);

create table if not exists horse_handicapping.day_card (
  card_id text primary key,
  track_id int references horse_handicapping.track(track_id),
  race_date date not null,
  created_at timestamptz default now()
);

create table if not exists horse_handicapping.race (
  race_id text primary key,
  track_id int references horse_handicapping.track(track_id),
  race_date date not null,
  race_number int not null,
  surface text,
  distance_yards int,
  track_condition text,
  field_size int,
  condition_text text,
  created_at timestamptz default now()
);

create unique index if not exists ux_race_natural on horse_handicapping.race(track_id, race_date, race_number);

create table if not exists horse_handicapping.entry (
  entry_id text,
  race_id text references horse_handicapping.race(race_id),
  program_number text,
  horse_name text,
  post_position int,
  trainer_name text,
  jockey_name text,
  ml_odds text,
  med_lasix boolean,
  equip_blinkers boolean,
  primary key (race_id, entry_id)
);

create unique index if not exists ux_entry_natural on horse_handicapping.entry(race_id, program_number);

create table if not exists horse_handicapping.result (
  race_id text,
  entry_id text,
  finish_pos int,
  final_time text,
  beaten_lengths numeric,
  odds_final numeric,
  dq_flag boolean,
  scratch_flag boolean,
  primary key (race_id, entry_id)
);

create table if not exists horse_handicapping.payout (
  race_id text,
  pool_type text,
  base_amount numeric,
  combination text,
  payout numeric,
  pool_total numeric,
  primary key (race_id, pool_type, combination)
);

create table if not exists horse_handicapping.workout (
  horse_name text,
  work_date date,
  track_code text,
  distance_furlongs numeric,
  surface text,
  course_type text,
  rank_in_set int,
  set_size int,
  time_raw text,
  bullet_flag boolean,
  primary key (horse_name, work_date, track_code, distance_furlongs)
);
