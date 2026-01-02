CREATE TABLE health_dw.silver.heart_rate (
  sk_date INT NOT NULL,
  sk_time STRING NOT NULL,
  timestamp TIMESTAMP NOT NULL,
  bpm BIGINT,
  source STRING,
  business_key_hash STRING NOT NULL,
  row_hash STRING NOT NULL,
  load_datetime TIMESTAMP NOT NULL,
  update_datetime TIMESTAMP NOT NULL
)
USING DELTA