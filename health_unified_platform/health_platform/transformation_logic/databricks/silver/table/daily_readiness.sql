CREATE TABLE health_dw.silver.daily_readiness (
  sk_date INTEGER NOT NULL,
  id STRING NOT NULL,
  day DATE NOT NULL,
  readiness_score BIGINT,
  temperature_deviation DOUBLE,
  temperature_trend_deviation DOUBLE,
  timestamp TIMESTAMP,
  activity_balance DOUBLE,
  body_temperature DOUBLE,
  hrv_balance DOUBLE,
  previous_day_activity DOUBLE,
  previous_night DOUBLE,
  recovery_index DOUBLE,
  resting_heart_rate DOUBLE,
  sleep_balance DOUBLE,
  sleep_regularity DOUBLE,
  row_hash STRING NOT NULL,
  load_datetime TIMESTAMP NOT NULL,
  update_datetime TIMESTAMP NOT NULL
)
USING DELTA