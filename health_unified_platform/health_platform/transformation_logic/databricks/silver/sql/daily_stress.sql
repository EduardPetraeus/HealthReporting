-- =============================================================================
-- daily_stress.sql
-- Silver: Oura daily stress summary
--
-- Source: health_dw.bronze.stg_oura_daily_stress
--
-- Business key: business_key_hash (day)
-- Change detection: row_hash over day_summary, stress_high, recovery_high
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.daily_stress (
    sk_date           INTEGER   NOT NULL,
    day               DATE      NOT NULL,
    day_summary       STRING,
    stress_high       INTEGER,
    recovery_high     INTEGER,
    business_key_hash STRING    NOT NULL,
    row_hash          STRING    NOT NULL,
    load_datetime     TIMESTAMP NOT NULL,
    update_datetime   TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.daily_stress_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY day
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM health_dw.bronze.stg_oura_daily_stress
    WHERE day IS NOT NULL
)
SELECT
    year(to_date(day)) * 10000
        + month(to_date(day)) * 100
        + dayofmonth(to_date(day))                        AS sk_date,
    to_date(day)                                          AS day,
    day_summary,
    CAST(stress_high AS INTEGER)                          AS stress_high,
    CAST(recovery_high AS INTEGER)                        AS recovery_high,
    sha2(coalesce(cast(day AS STRING), ''), 256)          AS business_key_hash,
    sha2(
        concat_ws('||',
            coalesce(day_summary, ''),
            coalesce(cast(stress_high AS STRING), ''),
            coalesce(cast(recovery_high AS STRING), '')
        ), 256
    )                                                     AS row_hash,
    current_timestamp()                                   AS load_datetime
FROM deduped
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO health_dw.silver.daily_stress AS target
USING health_dw.silver.daily_stress_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.sk_date         = source.sk_date,
        target.day             = source.day,
        target.day_summary     = source.day_summary,
        target.stress_high     = source.stress_high,
        target.recovery_high   = source.recovery_high,
        target.row_hash        = source.row_hash,
        target.update_datetime = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        sk_date, day, day_summary, stress_high, recovery_high,
        business_key_hash, row_hash, load_datetime, update_datetime
    )
    VALUES (
        source.sk_date, source.day, source.day_summary, source.stress_high,
        source.recovery_high, source.business_key_hash, source.row_hash,
        current_timestamp(), current_timestamp()
    );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.daily_stress_staging;
