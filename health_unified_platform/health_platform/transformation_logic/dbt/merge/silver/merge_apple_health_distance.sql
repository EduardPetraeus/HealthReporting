-- merge_apple_health_distance.sql
-- Per-source merge: Apple Health -> silver.distance
-- UNION ALL of walking/running + cycling distance, per session with distance_type
--
-- Sources:
--   bronze.stg_apple_health_distance_walking_running
--   bronze.stg_apple_health_distance_cycling
--
-- Usage: python run_merge.py silver/merge_apple_health_distance.sql

-- Step 1: UNION ALL both sources with deduplication per source
CREATE OR REPLACE TABLE silver.distance__staging AS
WITH walking_running AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY startDate, sourceName ORDER BY _ingested_at, startDate
    ) AS rn
    FROM bronze.stg_apple_health_distance_walking_running
    WHERE startDate IS NOT NULL
),
cycling AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY startDate, sourceName ORDER BY _ingested_at, startDate
    ) AS rn
    FROM bronze.stg_apple_health_distance_cycling
    WHERE startDate IS NOT NULL
),
combined AS (
    SELECT startDate, endDate, value, sourceName, duration_seconds,
           'walking_running' AS distance_type
    FROM walking_running WHERE rn = 1
    UNION ALL
    SELECT startDate, endDate, value, sourceName, duration_seconds,
           'cycling' AS distance_type
    FROM cycling WHERE rn = 1
)
SELECT
    (year(startDate) * 10000 + month(startDate) * 100 + day(startDate))::INTEGER AS sk_date,
    lpad(hour(startDate)::VARCHAR, 2, '0') || lpad(minute(startDate)::VARCHAR, 2, '0') AS sk_time,
    startDate::TIMESTAMP AS timestamp,
    ROUND(value::DOUBLE / 1000.0, 4) AS distance_km,
    distance_type,
    sourceName AS source_name,
    md5(
        coalesce(cast(startDate AS VARCHAR), '') || '||' ||
        coalesce(sourceName, '') || '||' ||
        distance_type
    ) AS business_key_hash,
    md5(
        cast(startDate AS VARCHAR) || '||' ||
        cast(value AS VARCHAR) || '||' ||
        distance_type || '||' ||
        sourceName
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM combined;

-- Step 2: Merge staging into silver.distance
MERGE INTO silver.distance AS target
USING silver.distance__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    sk_time           = src.sk_time,
    timestamp         = src.timestamp,
    distance_km       = src.distance_km,
    distance_type     = src.distance_type,
    source_name       = src.source_name,
    business_key_hash = src.business_key_hash,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, sk_time, timestamp, distance_km, distance_type, source_name,
    business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.sk_time, src.timestamp, src.distance_km, src.distance_type, src.source_name,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.distance__staging;
