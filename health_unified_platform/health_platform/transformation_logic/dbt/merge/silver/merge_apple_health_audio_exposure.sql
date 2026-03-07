-- merge_apple_health_audio_exposure.sql
-- Per-source merge: Apple Health -> silver.audio_exposure
-- Daily aggregation: avg/max dB per exposure type (environmental vs headphone)
--
-- Sources:
--   bronze.stg_apple_health_env_audio_exposure
--   bronze.stg_apple_health_headphone_audio_exposure
--
-- Usage: python run_merge.py silver/merge_apple_health_audio_exposure.sql

-- Step 1: Aggregate each source to daily level
CREATE OR REPLACE TABLE silver.audio_exposure__staging AS
WITH env_daily AS (
    SELECT
        startDate::DATE AS date,
        'environmental' AS exposure_type,
        ROUND(AVG(value::DOUBLE), 2) AS avg_db,
        ROUND(MAX(value::DOUBLE), 2) AS max_db,
        COUNT(*) AS sample_count
    FROM bronze.stg_apple_health_env_audio_exposure
    WHERE startDate IS NOT NULL
      AND value::DOUBLE > 0
    GROUP BY 1
),
headphone_daily AS (
    SELECT
        startDate::DATE AS date,
        'headphone' AS exposure_type,
        ROUND(AVG(value::DOUBLE), 2) AS avg_db,
        ROUND(MAX(value::DOUBLE), 2) AS max_db,
        COUNT(*) AS sample_count
    FROM bronze.stg_apple_health_headphone_audio_exposure
    WHERE startDate IS NOT NULL
      AND value::DOUBLE > 0
    GROUP BY 1
),
combined AS (
    SELECT * FROM env_daily
    UNION ALL
    SELECT * FROM headphone_daily
)
SELECT
    (year(date) * 10000 + month(date) * 100 + day(date))::INTEGER AS sk_date,
    date,
    exposure_type,
    avg_db,
    max_db,
    sample_count,
    'apple_health' AS source_name,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' ||
        exposure_type
    ) AS business_key_hash,
    md5(
        cast(date AS VARCHAR) || '||' ||
        exposure_type || '||' ||
        cast(avg_db AS VARCHAR) || '||' ||
        cast(max_db AS VARCHAR) || '||' ||
        cast(sample_count AS VARCHAR)
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM combined;

-- Step 2: Merge staging into silver.audio_exposure
MERGE INTO silver.audio_exposure AS target
USING silver.audio_exposure__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    date              = src.date,
    exposure_type     = src.exposure_type,
    avg_db            = src.avg_db,
    max_db            = src.max_db,
    sample_count      = src.sample_count,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, date, exposure_type, avg_db, max_db, sample_count, source_name,
    business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.date, src.exposure_type, src.avg_db, src.max_db, src.sample_count, src.source_name,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.audio_exposure__staging;
