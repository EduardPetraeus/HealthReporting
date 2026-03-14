-- merge_withings_body_temperature.sql
-- Per-source merge: Withings API + CSV -> silver.body_temperature (shared table)
-- UNION ALL from API (stg_withings_temperature) and CSV (stg_withings_body_temperature, stg_withings_body_temperature_csv)
-- Source-qualified staging table to avoid collisions with Apple Health merge.
--
-- Usage: python run_merge.py silver/merge_withings_body_temperature.sql

-- Step 1: Create staging table with UNION ALL from all sources
CREATE OR REPLACE TABLE silver.body_temperature__withings_staging AS
WITH api_source AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY datetime ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_temperature
    WHERE datetime IS NOT NULL
),
csv_source_legacy AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_body_temperature
    WHERE date IS NOT NULL
),
csv_source_new AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_body_temperature_csv
    WHERE date IS NOT NULL
),
combined AS (
    -- API temperature data
    SELECT
        datetime::TIMESTAMP AS timestamp,
        ROUND(temperature_c::DOUBLE, 1) AS temperature_degc,
        'withings' AS source_name
    FROM api_source WHERE rn = 1

    UNION ALL

    -- Legacy CSV path (stg_withings_body_temperature)
    SELECT
        date::TIMESTAMP AS timestamp,
        ROUND(TRY_CAST("value (°C)" AS DOUBLE), 1) AS temperature_degc,
        'withings' AS source_name
    FROM csv_source_legacy WHERE rn = 1

    UNION ALL

    -- New CSV path (stg_withings_body_temperature_csv)
    SELECT
        date::TIMESTAMP AS timestamp,
        ROUND(TRY_CAST("value (°C)" AS DOUBLE), 1) AS temperature_degc,
        'withings' AS source_name
    FROM csv_source_new WHERE rn = 1
),
final_dedup AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY timestamp ORDER BY temperature_degc DESC NULLS LAST) AS rn
    FROM combined
    WHERE timestamp IS NOT NULL
)
SELECT
    (year(timestamp::DATE) * 10000 + month(timestamp::DATE) * 100 + day(timestamp::DATE))::INTEGER AS sk_date,
    timestamp,
    temperature_degc,
    source_name,
    md5(
        coalesce(cast(timestamp AS VARCHAR), '') || '||' || 'withings'
    ) AS business_key_hash,
    md5(
        coalesce(cast(timestamp AS VARCHAR), '') || '||' ||
        coalesce(cast(temperature_degc AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM final_dedup
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
