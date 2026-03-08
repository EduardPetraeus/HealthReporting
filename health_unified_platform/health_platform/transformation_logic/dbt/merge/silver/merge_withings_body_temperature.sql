-- merge_withings_body_temperature.sql
-- Per-source merge: Withings CSV -> silver.body_temperature (shared table)
-- Source-qualified staging table to avoid collisions with Apple Health merge.
--
-- Usage: python run_merge.py silver/merge_withings_body_temperature.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.body_temperature__withings_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_withings_body_temperature
    WHERE date IS NOT NULL
)
SELECT
    (year(date::TIMESTAMP::DATE) * 10000 + month(date::TIMESTAMP::DATE) * 100 + day(date::TIMESTAMP::DATE))::INTEGER AS sk_date,
    date::TIMESTAMP AS timestamp,
    ROUND("value (°C)"::DOUBLE, 1) AS temperature_degc,
    'withings' AS source_name,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' || 'withings'
    ) AS business_key_hash,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' ||
        coalesce(cast("value (°C)" AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.body_temperature
MERGE INTO silver.body_temperature AS target
USING silver.body_temperature__withings_staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    timestamp         = src.timestamp,
    temperature_degc  = src.temperature_degc,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, timestamp, temperature_degc,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.timestamp, src.temperature_degc,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.body_temperature__withings_staging;
