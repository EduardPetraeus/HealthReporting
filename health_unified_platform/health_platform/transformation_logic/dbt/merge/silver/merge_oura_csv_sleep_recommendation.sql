-- merge_oura_csv_sleep_recommendation.sql
-- Per-source merge: Oura CSV -> silver.sleep_recommendation
-- Follows the staging + MERGE + drop pattern, adapted for DuckDB.
--
-- Usage: python run_merge.py silver/merge_oura_csv_sleep_recommendation.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.sleep_recommendation__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY day
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_oura_csv_sleeptime
    WHERE day IS NOT NULL
)
SELECT
    (year(day::DATE) * 10000 + month(day::DATE) * 100 + day(day::DATE))::INTEGER AS sk_date,
    day::DATE AS date,
    recommendation,
    status,
    CASE WHEN optimal_bedtime IS NOT NULL AND optimal_bedtime != ''
         THEN json_extract_string(optimal_bedtime, '$.start_offset')::INTEGER
         ELSE NULL END AS optimal_start_offset,
    CASE WHEN optimal_bedtime IS NOT NULL AND optimal_bedtime != ''
         THEN json_extract_string(optimal_bedtime, '$.end_offset')::INTEGER
         ELSE NULL END AS optimal_end_offset,
    'oura' AS source_name,
    md5(
        coalesce(cast(day AS VARCHAR), '') || '||' || 'oura'
    ) AS business_key_hash,
    md5(
        coalesce(cast(day AS VARCHAR), '') || '||' ||
        coalesce(recommendation, '') || '||' ||
        coalesce(status, '') || '||' ||
        coalesce(cast(optimal_bedtime AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.sleep_recommendation
MERGE INTO silver.sleep_recommendation AS target
USING silver.sleep_recommendation__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date              = src.sk_date,
    date                 = src.date,
    recommendation       = src.recommendation,
    status               = src.status,
    optimal_start_offset = src.optimal_start_offset,
    optimal_end_offset   = src.optimal_end_offset,
    source_name          = src.source_name,
    row_hash             = src.row_hash,
    update_datetime      = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, date, recommendation, status, optimal_start_offset, optimal_end_offset,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.date, src.recommendation, src.status, src.optimal_start_offset, src.optimal_end_offset,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.sleep_recommendation__staging;
