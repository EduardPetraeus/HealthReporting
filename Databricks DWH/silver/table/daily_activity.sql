CREATE TABLE health_dw.silver.daily_activity (
  sk_date INTEGER NOT NULL,
  id STRING NOT NULL,
  day DATE NOT NULL,
  activity_score BIGINT,
  steps BIGINT,
  equivalent_walking_distance BIGINT,
  inactivity_alerts BIGINT,
  target_calories BIGINT,
  active_calories BIGINT,
  meet_daily_targets DOUBLE,
  move_every_hour DOUBLE,
  recovery_time DOUBLE,
  stay_active DOUBLE,
  training_frequency DOUBLE,
  training_volume DOUBLE,
  row_hash STRING NOT NULL,
  load_datetime TIMESTAMP NOT NULL,
  update_datetime TIMESTAMP NOT NULL
)
USING DELTA