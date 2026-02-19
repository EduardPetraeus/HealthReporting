-- merge_apple_health_mindful_session.sql
-- Per-source merge: Apple Health -> silver.mindful_session
--
-- Usage: python run_merge.py silver/merge_apple_health_mindful_session.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.mindful_session__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY startDate, sourceName
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_apple_health_mindful_session
    WHERE startDate IS NOT NULL
)
SELECT
    (year(startDate::TIMESTAMP) * 10000 + month(startDate::TIMESTAMP) * 100 + day(startDate::TIMESTAMP))::INTEGER AS sk_date,
    lpad(hour(startDate::TIMESTAMP)::VARCHAR, 2, '0') || lpad(minute(startDate::TIMESTAMP)::VARCHAR, 2, '0') AS sk_time,
    startDate::TIMESTAMP   AS timestamp,
    strptime(endDate, '%Y-%m-%d %H:%M:%S %z')::TIMESTAMP AS end_timestamp,
    duration_seconds,
    sourceName             AS source_name,
    md5(
        coalesce(cast(startDate AS VARCHAR), '') || '||' ||
        coalesce(sourceName, '')
    )                      AS business_key_hash,
    md5(
        coalesce(cast(startDate AS VARCHAR), '') || '||' ||
        coalesce(cast(endDate AS VARCHAR), '') || '||' ||
        coalesce(cast(duration_seconds AS VARCHAR), '') || '||' ||
        coalesce(sourceName, '')
    )                      AS row_hash,
    current_timestamp      AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.mindful_session
MERGE INTO silver.mindful_session AS target
USING silver.mindful_session__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    sk_time           = src.sk_time,
    timestamp         = src.timestamp,
    end_timestamp     = src.end_timestamp,
    duration_seconds  = src.duration_seconds,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, sk_time, timestamp, end_timestamp,
    duration_seconds, source_name,
    business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.sk_time, src.timestamp, src.end_timestamp,
    src.duration_seconds, src.source_name,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.mindful_session__staging;
