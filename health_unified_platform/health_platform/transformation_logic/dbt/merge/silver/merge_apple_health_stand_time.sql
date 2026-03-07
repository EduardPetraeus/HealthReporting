-- merge_apple_health_stand_time.sql
-- Per-source merge: Apple Health -> silver.stand_time
-- Daily aggregation: stand_time (minutes) + stand_hours from two bronze sources
--
-- Sources:
--   bronze.stg_apple_health_stand_time (minutes per session)
--   bronze.stg_apple_health_stand_hour (hourly stand events)
--
-- Usage: python run_merge.py silver/merge_apple_health_stand_time.sql

-- Step 1: Aggregate each source to daily level, then full outer join
CREATE OR REPLACE TABLE silver.stand_time__staging AS
WITH stand_minutes_daily AS (
    SELECT
        startDate::DATE AS date,
        SUM(value::DOUBLE) AS stand_minutes
    FROM bronze.stg_apple_health_stand_time
    WHERE startDate IS NOT NULL
      AND value::DOUBLE > 0
    GROUP BY 1
),
stand_hours_daily AS (
    SELECT
        startDate::DATE AS date,
        COUNT(*)::INTEGER AS stand_hours
    FROM bronze.stg_apple_health_stand_hour
    WHERE startDate IS NOT NULL
    GROUP BY 1
),
combined AS (
    SELECT
        coalesce(m.date, h.date) AS date,
        m.stand_minutes,
        h.stand_hours
    FROM stand_minutes_daily m
    FULL OUTER JOIN stand_hours_daily h ON m.date = h.date
)
SELECT
    (year(date) * 10000 + month(date) * 100 + day(date))::INTEGER AS sk_date,
    date,
    ROUND(stand_minutes, 1) AS stand_minutes,
    coalesce(stand_hours, 0) AS stand_hours,
    'apple_health' AS source_name,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' || 'apple_health'
    ) AS business_key_hash,
    md5(
        coalesce(cast(stand_minutes AS VARCHAR), '') || '||' ||
        coalesce(cast(stand_hours AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM combined;

-- Step 2: Merge staging into silver.stand_time
MERGE INTO silver.stand_time AS target
USING silver.stand_time__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    date              = src.date,
    stand_minutes     = src.stand_minutes,
    stand_hours       = src.stand_hours,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, date, stand_minutes, stand_hours, source_name,
    business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.date, src.stand_minutes, src.stand_hours, src.source_name,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.stand_time__staging;
