-- merge_apple_health_heart_rate.sql
-- Per-source merge: Apple Health -> silver.heart_rate
-- Follows the Databricks staging + MERGE + drop pattern, adapted for DuckDB.
--
-- Usage: python run_merge.py silver/merge_apple_health_heart_rate.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.heart_rate__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY startDate, sourceName
            ORDER BY startDate
        ) AS rn
    FROM bronze.stg_apple_health_heart_rate
    WHERE startDate IS NOT NULL
)
SELECT
    (year(startDate) * 10000 + month(startDate) * 100 + day(startDate))::INTEGER AS sk_date,
    lpad(hour(startDate)::VARCHAR, 2, '0') || lpad(minute(startDate)::VARCHAR, 2, '0') AS sk_time,
    startDate::TIMESTAMP AS timestamp,
    value::INTEGER AS bpm,
    sourceName AS source,
    md5(
        coalesce(cast(startDate AS VARCHAR), '') || '||' || coalesce(sourceName, '')
    ) AS business_key_hash,
    md5(
        cast(startDate AS VARCHAR) || '||' || cast(value AS VARCHAR) || '||' || sourceName
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.heart_rate
MERGE INTO silver.heart_rate AS target
USING silver.heart_rate__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    sk_time           = src.sk_time,
    timestamp         = src.timestamp,
    bpm               = src.bpm,
    source            = src.source,
    business_key_hash = src.business_key_hash,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date,
    sk_time,
    timestamp,
    bpm,
    source,
    business_key_hash,
    row_hash,
    load_datetime,
    update_datetime
  )
  VALUES (
    src.sk_date,
    src.sk_time,
    src.timestamp,
    src.bpm,
    src.source,
    src.business_key_hash,
    src.row_hash,
    current_timestamp,
    current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.heart_rate__staging;
