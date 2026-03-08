-- =============================================================================
-- resting_heart_rate.sql
-- Silver: Apple Health resting heart rate daily estimate
--
-- Source: health_dw.bronze.stg_apple_health_resting_heart_rate
--
-- Business key: business_key_hash (date + source_name)
-- Change detection: row_hash over resting_hr_bpm
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.resting_heart_rate (
    sk_date           INTEGER   NOT NULL,
    date              DATE      NOT NULL,
    resting_hr_bpm    DOUBLE,
    source_name       STRING,
    business_key_hash STRING    NOT NULL,
    row_hash          STRING    NOT NULL,
    load_datetime     TIMESTAMP NOT NULL,
    update_datetime   TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.resting_heart_rate_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY startDate, sourceName
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM health_dw.bronze.stg_apple_health_resting_heart_rate
    WHERE startDate IS NOT NULL
)
SELECT
    year(to_timestamp(startDate)) * 10000
        + month(to_timestamp(startDate)) * 100
        + dayofmonth(to_timestamp(startDate))             AS sk_date,
    to_date(startDate)                                    AS date,
    CAST(value AS DOUBLE)                                 AS resting_hr_bpm,
    sourceName                                            AS source_name,
    sha2(
        concat_ws('||',
            coalesce(cast(startDate AS STRING), ''),
            coalesce(sourceName, '')
        ), 256
    )                                                     AS business_key_hash,
    sha2(
        coalesce(cast(value AS STRING), ''), 256
    )                                                     AS row_hash,
    current_timestamp()                                   AS load_datetime
FROM deduped
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO health_dw.silver.resting_heart_rate AS target
USING health_dw.silver.resting_heart_rate_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.sk_date           = source.sk_date,
        target.date              = source.date,
        target.resting_hr_bpm    = source.resting_hr_bpm,
        target.source_name       = source.source_name,
        target.row_hash          = source.row_hash,
        target.update_datetime   = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        sk_date, date, resting_hr_bpm, source_name,
        business_key_hash, row_hash, load_datetime, update_datetime
    )
    VALUES (
        source.sk_date, source.date, source.resting_hr_bpm, source.source_name,
        source.business_key_hash, source.row_hash,
        current_timestamp(), current_timestamp()
    );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.resting_heart_rate_staging;
