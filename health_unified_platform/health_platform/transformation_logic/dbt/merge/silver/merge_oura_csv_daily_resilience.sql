-- merge_oura_csv_daily_resilience.sql
-- Per-source merge: Oura CSV -> silver.daily_resilience
-- Follows the staging + MERGE + drop pattern, adapted for DuckDB.
--
-- Usage: python run_merge.py silver/merge_oura_csv_daily_resilience.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.daily_resilience__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY day
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_oura_csv_dailyresilience
    WHERE day IS NOT NULL
)
SELECT
    (year(day::DATE) * 10000 + month(day::DATE) * 100 + day(day::DATE))::INTEGER AS sk_date,
    day::DATE AS date,
    json_extract_string(contributors, '$.sleep_recovery')::DOUBLE AS sleep_recovery,
    json_extract_string(contributors, '$.daytime_recovery')::DOUBLE AS daytime_recovery,
    json_extract_string(contributors, '$.stress')::DOUBLE AS stress,
    level,
    'oura' AS source_name,
    md5(
        coalesce(cast(day AS VARCHAR), '') || '||' || 'oura'
    ) AS business_key_hash,
    md5(
        coalesce(cast(day AS VARCHAR), '') || '||' ||
        coalesce(cast(json_extract_string(contributors, '$.sleep_recovery') AS VARCHAR), '') || '||' ||
        coalesce(cast(json_extract_string(contributors, '$.daytime_recovery') AS VARCHAR), '') || '||' ||
        coalesce(cast(json_extract_string(contributors, '$.stress') AS VARCHAR), '') || '||' ||
        coalesce(level, '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.daily_resilience
MERGE INTO silver.daily_resilience AS target
USING silver.daily_resilience__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    date              = src.date,
    sleep_recovery    = src.sleep_recovery,
    daytime_recovery  = src.daytime_recovery,
    stress            = src.stress,
    level             = src.level,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, date, sleep_recovery, daytime_recovery, stress, level,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.date, src.sleep_recovery, src.daytime_recovery, src.stress, src.level,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.daily_resilience__staging;
