CREATE TABLE health_dw.silver.blood_oxygen (
  sk_date INTEGER NOT NULL,
  id STRING NOT NULL,
  day DATE NOT NULL,
  breathing_disturbance_index DOUBLE,
  measurement_type STRING NOT NULL,
  spo2_percentage DOUBLE,
  row_hash STRING NOT NULL,
  load_datetime TIMESTAMP NOT NULL,
  update_datetime TIMESTAMP NOT NULL
)
USING DELTA