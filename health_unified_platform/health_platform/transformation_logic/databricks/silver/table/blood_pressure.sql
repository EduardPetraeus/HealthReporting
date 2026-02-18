CREATE TABLE health_dw.silver.blood_pressure (
  sk_date INTEGER NOT NULL,
  sk_time STRING NOT NULL,
  datetime TIMESTAMP NOT NULL,
  systolic BIGINT NOT NULL,
  diastolic BIGINT NOT NULL,
  comments STRING,
  business_key_hash STRING NOT NULL,
  row_hash STRING NOT NULL,
  load_datetime TIMESTAMP NOT NULL,
  update_datetime TIMESTAMP NOT NULL
)
USING DELTA