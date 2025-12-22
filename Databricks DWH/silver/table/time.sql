%sql
CREATE TABLE health_dw.silver.time (
    sk_time STRING,
    time_code STRING,
    hour_12_code STRING,
    hour_12_key INT,
    minute_code STRING,
    minute_key INT,
    ampm_code STRING,
    hour_24_code STRING,
    hour_24_key INT,
    minute_15_code STRING,
    minute_15_key INT
);