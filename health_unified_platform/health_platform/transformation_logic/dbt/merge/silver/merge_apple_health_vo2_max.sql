-- merge_apple_health_vo2_max.sql
-- Per-source merge: Apple Health -> silver.vo2_max
-- VO2 Max cardio fitness estimate, daily grain
-- Shared table: source_name in BK for future Oura data (A6)
--
-- Usage: python run_merge.py silver/merge_apple_health_vo2_max.sql

-- Step 1: Create staging table with daily deduplication
CREATE OR REPLACE TABLE silver.vo2_max__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY startDate::DATE
            ORDER BY _ingested_at DESC, startDate DESC
        ) AS rn
    FROM bronze.stg_apple_health_vo2_max
    WHERE startDate IS NOT NULL
      AND value::DOUBLE > 0
)
SELECT
    (year(startDate) * 10000 + month(startDate) * 100 + day(startDate))::INTEGER AS sk_date,
    startDate::DATE AS date,
    value::DOUBLE AS vo2_max_ml_kg_min,
    'apple_health' AS source_name,
    md5(
        coalesce(cast(startDate::DATE AS VARCHAR), '') || '||' || 'apple_health'
    ) AS business_key_hash,
    md5(
        cast(startDate::DATE AS VARCHAR) || '||' ||
        cast(value AS VARCHAR)
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.vo2_max
MERGE INTO silver.vo2_max AS target
USING silver.vo2_max__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date              = src.sk_date,
    date                 = src.date,
    vo2_max_ml_kg_min    = src.vo2_max_ml_kg_min,
    source_name          = src.source_name,
    row_hash             = src.row_hash,
    update_datetime      = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, date, vo2_max_ml_kg_min, source_name,
    business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.date, src.vo2_max_ml_kg_min, src.source_name,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.vo2_max__staging;
