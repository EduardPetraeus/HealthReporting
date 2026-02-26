CREATE TABLE health_dw.silver.daily_sleep (
  sk_date INTEGER NOT NULL,
  id STRING NOT NULL,
  day DATE NOT NULL,
  sleep_score BIGINT,
  timestamp TIMESTAMP,
  deep_sleep DOUBLE,
  efficiency DOUBLE,
  latency DOUBLE,
  rem_sleep DOUBLE,
  restfulness DOUBLE,
  timing DOUBLE,
  total_sleep DOUBLE,
  row_hash STRING NOT NULL,
  load_datetime TIMESTAMP NOT NULL,
  update_datetime TIMESTAMP NOT NULL
)
USING DELTA