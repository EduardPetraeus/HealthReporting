-- merge_oura_csv_daytime_stress.sql
-- Per-source merge: Oura CSV -> silver.daytime_stress
-- Aggregates intraday stress samples to daily level.
--
-- Usage: python run_merge.py silver/merge_oura_csv_daytime_stress.sql

-- Step 1: Create staging table with daily aggregation
CREATE OR REPLACE TABLE silver.daytime_stress__staging AS
WITH daily AS (
    SELECT
        timestamp::TIMESTAMP::DATE AS date,
        ROUND(AVG(TRY_CAST(stress_value AS DOUBLE)), 1) AS avg_stress,
        ROUND(MAX(TRY_CAST(stress_value AS DOUBLE)), 1) AS max_stress,
        ROUND(AVG(TRY_CAST(recovery_value AS DOUBLE)), 1) AS avg_recovery,
        ROUND(MAX(TRY_CAST(recovery_value AS DOUBLE)), 1) AS max_recovery,
        COUNT(*) AS sample_count
    FROM bronze.stg_oura_csv_daytimestress
    WHERE timestamp IS NOT NULL
    GROUP BY timestamp::TIMESTAMP::DATE
)
SELECT
    (year(date) * 10000 + month(date) * 100 + day(date))::INTEGER AS sk_date,
    date,
    avg_stress,
    max_stress,
    avg_recovery,
    max_recovery,
    sample_count,
    'oura' AS source_name,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' || 'oura'
    ) AS business_key_hash,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' ||
        coalesce(cast(avg_stress AS VARCHAR), '') || '||' ||
        coalesce(cast(max_stress AS VARCHAR), '') || '||' ||
        coalesce(cast(avg_recovery AS VARCHAR), '') || '||' ||
        coalesce(cast(max_recovery AS VARCHAR), '') || '||' ||
        coalesce(cast(sample_count AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM daily;

-- Step 2: Merge staging into silver.daytime_stress
MERGE INTO silver.daytime_stress AS target
USING silver.daytime_stress__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    date              = src.date,
    avg_stress        = src.avg_stress,
    max_stress        = src.max_stress,
    avg_recovery      = src.avg_recovery,
    max_recovery      = src.max_recovery,
    sample_count      = src.sample_count,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, date, avg_stress, max_stress, avg_recovery, max_recovery, sample_count,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.date, src.avg_stress, src.max_stress, src.avg_recovery, src.max_recovery, src.sample_count,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.daytime_stress__staging;
