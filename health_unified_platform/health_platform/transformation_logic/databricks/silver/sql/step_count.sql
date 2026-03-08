-- =============================================================================
-- step_count.sql
-- Silver: Apple Health step count measurements
--
-- Source: health_dw.bronze.stg_apple_health_step_count
--
-- Business key: business_key_hash (startDate + sourceName)
-- Change detection: row_hash over all measurement fields
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.step_count (
    sk_date           INTEGER   NOT NULL,
    sk_time           STRING    NOT NULL,
    timestamp         TIMESTAMP,
    end_timestamp     TIMESTAMP,
    duration_seconds  DOUBLE,
    step_count        INTEGER,
    source_name       STRING,
    business_key_hash STRING    NOT NULL,
    row_hash          STRING    NOT NULL,
    load_datetime     TIMESTAMP NOT NULL,
    update_datetime   TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.step_count_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY startDate, sourceName
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM health_dw.bronze.stg_apple_health_step_count
    WHERE startDate IS NOT NULL
)
SELECT
    year(to_timestamp(startDate)) * 10000
        + month(to_timestamp(startDate)) * 100
        + dayofmonth(to_timestamp(startDate))             AS sk_date,
    lpad(hour(to_timestamp(startDate)), 2, '0')
        || lpad(minute(to_timestamp(startDate)), 2, '0')  AS sk_time,
    to_timestamp(startDate)                                AS timestamp,
    to_timestamp(endDate)                                  AS end_timestamp,
    CAST(duration_seconds AS DOUBLE)                       AS duration_seconds,
    CAST(value AS INTEGER)                                 AS step_count,
    sourceName                                             AS source_name,
    sha2(
        concat_ws('||',
            coalesce(cast(startDate AS STRING), ''),
            coalesce(sourceName, '')
        ), 256
    )                                                      AS business_key_hash,
    sha2(
        concat_ws('||',
            coalesce(cast(startDate AS STRING), ''),
            coalesce(cast(endDate AS STRING), ''),
            coalesce(cast(duration_seconds AS STRING), ''),
            coalesce(cast(value AS STRING), ''),
            coalesce(sourceName, '')
        ), 256
    )                                                      AS row_hash,
    current_timestamp()                                    AS load_datetime
FROM deduped
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO health_dw.silver.step_count AS target
USING health_dw.silver.step_count_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.sk_date           = source.sk_date,
        target.sk_time           = source.sk_time,
        target.timestamp         = source.timestamp,
        target.end_timestamp     = source.end_timestamp,
        target.duration_seconds  = source.duration_seconds,
        target.step_count        = source.step_count,
        target.source_name       = source.source_name,
        target.row_hash          = source.row_hash,
        target.update_datetime   = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        sk_date, sk_time, timestamp, end_timestamp, duration_seconds,
        step_count, source_name, business_key_hash, row_hash,
        load_datetime, update_datetime
    )
    VALUES (
        source.sk_date, source.sk_time, source.timestamp, source.end_timestamp,
        source.duration_seconds, source.step_count, source.source_name,
        source.business_key_hash, source.row_hash,
        current_timestamp(), current_timestamp()
    );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.step_count_staging;
