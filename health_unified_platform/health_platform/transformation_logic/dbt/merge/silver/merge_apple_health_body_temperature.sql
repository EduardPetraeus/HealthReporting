-- merge_apple_health_body_temperature.sql
-- Per-source merge: Apple Health (Withings) -> silver.body_temperature
--
-- Usage: python run_merge.py silver/merge_apple_health_body_temperature.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.body_temperature__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY startDate, sourceName
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_apple_health_body_temperature
    WHERE startDate IS NOT NULL
      AND value IS NOT NULL
)
SELECT
    (year(startDate::TIMESTAMP) * 10000 + month(startDate::TIMESTAMP) * 100 + day(startDate::TIMESTAMP))::INTEGER AS sk_date,
    lpad(hour(startDate::TIMESTAMP)::VARCHAR, 2, '0') || lpad(minute(startDate::TIMESTAMP)::VARCHAR, 2, '0') AS sk_time,
    startDate::TIMESTAMP   AS timestamp,
    value::DOUBLE          AS temperature_degc,
    sourceName             AS source_name,
    md5(
        coalesce(cast(startDate AS VARCHAR), '') || '||' ||
        coalesce(sourceName, '')
    )                      AS business_key_hash,
    md5(
        coalesce(cast(startDate AS VARCHAR), '') || '||' ||
        coalesce(cast(value AS VARCHAR), '') || '||' ||
        coalesce(sourceName, '')
    )                      AS row_hash,
    current_timestamp      AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.body_temperature
MERGE INTO silver.body_temperature AS target
USING silver.body_temperature__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    sk_time           = src.sk_time,
    timestamp         = src.timestamp,
    temperature_degc  = src.temperature_degc,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, sk_time, timestamp, temperature_degc,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.sk_time, src.timestamp, src.temperature_degc,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.body_temperature__staging;
