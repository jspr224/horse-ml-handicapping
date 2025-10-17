
-- Minimal core tables (extend as needed)
create schema if not exists horse_handicapping;

create table if not exists horse_handicapping.race (
  race_id text primary key,
  track_code text not null,
  race_date date not null,
  race_num int not null,
  surface text,
  distance_yards int,
  field_size int,
  condition_text text,
  track_condition text,
  created_at timestamptz default now()
);

create table if not exists horse_handicapping.entry (
  race_id text references horse_handicapping.race(race_id),
  entry_id text,
  program_number text,
  horse_name text,
  post_position int,
  jockey_name text,
  trainer_name text,
  ml_odds text,
  morning_line_prob numeric,
  primary key (race_id, entry_id)
);

create table if not exists horse_handicapping.result (
  race_id text,
  entry_id text,
  finish_pos int,
  final_time numeric,
  beaten_lengths numeric,
  odds_final numeric,
  implied_prob_parimutuel numeric,
  dq_flag boolean,
  scratch_flag boolean,
  primary key (race_id, entry_id)
);

create table if not exists horse_handicapping.payouts (
  race_id text,
  pool_type text,
  combo text,
  payout numeric
);

create table if not exists horse_handicapping.features_base (
  race_id text,
  entry_id text,
  label_win int,
  label_place int,
  label_show int,
  primary key (race_id, entry_id)
);
