-- =============================================================================
-- daily_resilience.sql
-- Silver: Oura daily resilience score
--
-- Source: health_dw.bronze.stg_oura_daily_resilience
--
-- Business key: business_key_hash (day)
-- Change detection: row_hash over recovery scores and level
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.daily_resilience (
    sk_date             INTEGER   NOT NULL,
    date                DATE      NOT NULL,
    sleep_recovery      DOUBLE,
    daytime_recovery    DOUBLE,
    stress              DOUBLE,
    level               STRING,
    source_name         STRING,
    business_key_hash   STRING    NOT NULL,
    row_hash            STRING    NOT NULL,
    load_datetime       TIMESTAMP NOT NULL,
    update_datetime     TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.daily_resilience_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY day
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM health_dw.bronze.stg_oura_daily_resilience
    WHERE day IS NOT NULL
)
SELECT
    year(to_date(day)) * 10000
        + month(to_date(day)) * 100
        + dayofmonth(to_date(day))                        AS sk_date,
    to_date(day)                                          AS date,
    CAST(sleep_recovery AS DOUBLE)                        AS sleep_recovery,
    CAST(daytime_recovery AS DOUBLE)                      AS daytime_recovery,
    CAST(stress AS DOUBLE)                                AS stress,
    level,
    'oura'                                                AS source_name,
    sha2(coalesce(cast(day AS STRING), ''), 256)          AS business_key_hash,
    sha2(
        concat_ws('||',
            coalesce(cast(sleep_recovery AS STRING), ''),
            coalesce(cast(daytime_recovery AS STRING), ''),
            coalesce(cast(stress AS STRING), ''),
            coalesce(level, '')
        ), 256
    )                                                     AS row_hash,
    current_timestamp()                                   AS load_datetime
FROM deduped
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO health_dw.silver.daily_resilience AS target
USING health_dw.silver.daily_resilience_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.sk_date           = source.sk_date,
        target.date              = source.date,
        target.sleep_recovery    = source.sleep_recovery,
        target.daytime_recovery  = source.daytime_recovery,
        target.stress            = source.stress,
        target.level             = source.level,
        target.source_name       = source.source_name,
        target.row_hash          = source.row_hash,
        target.update_datetime   = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        sk_date, date, sleep_recovery, daytime_recovery, stress, level,
        source_name, business_key_hash, row_hash, load_datetime, update_datetime
    )
    VALUES (
        source.sk_date, source.date, source.sleep_recovery, source.daytime_recovery,
        source.stress, source.level, source.source_name,
        source.business_key_hash, source.row_hash,
        current_timestamp(), current_timestamp()
    );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.daily_resilience_staging;
