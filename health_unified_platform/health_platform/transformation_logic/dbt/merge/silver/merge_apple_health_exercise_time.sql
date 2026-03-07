-- merge_apple_health_exercise_time.sql
-- Per-source merge: Apple Health -> silver.exercise_time
-- Exercise time per session in minutes
--
-- Usage: python run_merge.py silver/merge_apple_health_exercise_time.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.exercise_time__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY startDate, sourceName
            ORDER BY _ingested_at, startDate
        ) AS rn
    FROM bronze.stg_apple_health_exercise_time
    WHERE startDate IS NOT NULL
)
SELECT
    (year(startDate) * 10000 + month(startDate) * 100 + day(startDate))::INTEGER AS sk_date,
    lpad(hour(startDate)::VARCHAR, 2, '0') || lpad(minute(startDate)::VARCHAR, 2, '0') AS sk_time,
    startDate::TIMESTAMP AS timestamp,
    value::DOUBLE AS exercise_minutes,
    sourceName AS source_name,
    md5(
        coalesce(cast(startDate AS VARCHAR), '') || '||' || coalesce(sourceName, '')
    ) AS business_key_hash,
    md5(
        cast(startDate AS VARCHAR) || '||' ||
        cast(value AS VARCHAR) || '||' ||
        sourceName
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.exercise_time
MERGE INTO silver.exercise_time AS target
USING silver.exercise_time__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    sk_time           = src.sk_time,
    timestamp         = src.timestamp,
    exercise_minutes  = src.exercise_minutes,
    source_name       = src.source_name,
    business_key_hash = src.business_key_hash,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, sk_time, timestamp, exercise_minutes, source_name,
    business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.sk_time, src.timestamp, src.exercise_minutes, src.source_name,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.exercise_time__staging;
