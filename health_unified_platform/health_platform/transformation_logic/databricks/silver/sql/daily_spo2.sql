-- =============================================================================
-- daily_spo2.sql
-- Silver: Oura daily SpO2 readings
--
-- Source: health_dw.bronze.stg_oura_daily_spo2
--
-- Business key: business_key_hash (day)
-- Change detection: row_hash over spo2_avg_pct, breathing_disturbance_index
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.daily_spo2 (
    sk_date                     INTEGER   NOT NULL,
    day                         DATE      NOT NULL,
    spo2_avg_pct                DOUBLE,
    breathing_disturbance_index DOUBLE,
    business_key_hash           STRING    NOT NULL,
    row_hash                    STRING    NOT NULL,
    load_datetime               TIMESTAMP NOT NULL,
    update_datetime             TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.daily_spo2_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY day
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM health_dw.bronze.stg_oura_daily_spo2
    WHERE day IS NOT NULL
)
SELECT
    year(to_date(day)) * 10000
        + month(to_date(day)) * 100
        + dayofmonth(to_date(day))                        AS sk_date,
    to_date(day)                                          AS day,
    CAST(spo2_avg_pct AS DOUBLE)                          AS spo2_avg_pct,
    CAST(breathing_disturbance_index AS DOUBLE)           AS breathing_disturbance_index,
    sha2(coalesce(cast(day AS STRING), ''), 256)          AS business_key_hash,
    sha2(
        concat_ws('||',
            coalesce(cast(spo2_avg_pct AS STRING), ''),
            coalesce(cast(breathing_disturbance_index AS STRING), '')
        ), 256
    )                                                     AS row_hash,
    current_timestamp()                                   AS load_datetime
FROM deduped
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO health_dw.silver.daily_spo2 AS target
USING health_dw.silver.daily_spo2_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.sk_date                      = source.sk_date,
        target.day                          = source.day,
        target.spo2_avg_pct                 = source.spo2_avg_pct,
        target.breathing_disturbance_index  = source.breathing_disturbance_index,
        target.row_hash                     = source.row_hash,
        target.update_datetime              = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        sk_date, day, spo2_avg_pct, breathing_disturbance_index,
        business_key_hash, row_hash, load_datetime, update_datetime
    )
    VALUES (
        source.sk_date, source.day, source.spo2_avg_pct,
        source.breathing_disturbance_index,
        source.business_key_hash, source.row_hash,
        current_timestamp(), current_timestamp()
    );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.daily_spo2_staging;
