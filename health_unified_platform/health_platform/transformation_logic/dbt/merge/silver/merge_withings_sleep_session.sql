-- merge_withings_sleep_session.sql
-- Per-source merge: Withings CSV -> silver.sleep_session (shared table)
-- Source-qualified staging table to avoid collisions with Oura merge.
-- Withings provides snoring data and HR stats that Oura does not.
--
-- Usage: python run_merge.py silver/merge_withings_sleep_session.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.sleep_session__withings_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY "from"
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_withings_sleep
    WHERE "from" IS NOT NULL
)
SELECT
    (year("from"::TIMESTAMP::DATE) * 10000 + month("from"::TIMESTAMP::DATE) * 100 + day("from"::TIMESTAMP::DATE))::INTEGER AS sk_date,
    "from"::TIMESTAMP::DATE AS date,
    "from"::TIMESTAMP AS bedtime_start,
    "to"::TIMESTAMP AS bedtime_end,
    ROUND((TRY_CAST("Light sleep (s)" AS DOUBLE) + TRY_CAST("Deep sleep (s)" AS DOUBLE) + TRY_CAST("REM sleep (s)" AS DOUBLE)) / 60.0, 1) AS total_sleep_min,
    ROUND(TRY_CAST("Deep sleep (s)" AS DOUBLE) / 60.0, 1) AS deep_sleep_min,
    ROUND(TRY_CAST("REM sleep (s)" AS DOUBLE) / 60.0, 1) AS rem_sleep_min,
    ROUND(TRY_CAST("Light sleep (s)" AS DOUBLE) / 60.0, 1) AS light_sleep_min,
    ROUND(TRY_CAST("Duration to wake up (s)" AS DOUBLE) / 60.0, 1) AS awake_min,
    NULL::INTEGER AS efficiency,
    ROUND(TRY_CAST("Duration to sleep (s)" AS DOUBLE) / 60.0, 1) AS latency_min,
    ROUND(TRY_CAST("Average heart rate" AS DOUBLE), 1) AS avg_hr,
    TRY_CAST("Heart rate (min)" AS INTEGER) AS min_hr,
    TRY_CAST("Heart rate (max)" AS INTEGER) AS max_hr,
    NULL::DOUBLE AS avg_hrv,
    TRY_CAST("Heart rate (min)" AS INTEGER) AS lowest_hr,
    NULL::INTEGER AS readiness_score,
    ROUND(TRY_CAST("Snoring (s)" AS DOUBLE) / 60.0, 1) AS snoring_min,
    TRY_CAST("Snoring episodes" AS INTEGER) AS snoring_episodes,
    'withings' AS source_name,
    md5(
        coalesce(cast("from" AS VARCHAR), '') || '||' || 'withings'
    ) AS business_key_hash,
    md5(
        coalesce(cast("from" AS VARCHAR), '') || '||' ||
        coalesce(cast("to" AS VARCHAR), '') || '||' ||
        coalesce(cast("Light sleep (s)" AS VARCHAR), '') || '||' ||
        coalesce(cast("Deep sleep (s)" AS VARCHAR), '') || '||' ||
        coalesce(cast("REM sleep (s)" AS VARCHAR), '') || '||' ||
        coalesce(cast("Average heart rate" AS VARCHAR), '') || '||' ||
        coalesce(cast("Snoring (s)" AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.sleep_session
MERGE INTO silver.sleep_session AS target
USING silver.sleep_session__withings_staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    date              = src.date,
    bedtime_start     = src.bedtime_start,
    bedtime_end       = src.bedtime_end,
    total_sleep_min   = src.total_sleep_min,
    deep_sleep_min    = src.deep_sleep_min,
    rem_sleep_min     = src.rem_sleep_min,
    light_sleep_min   = src.light_sleep_min,
    awake_min         = src.awake_min,
    efficiency        = src.efficiency,
    latency_min       = src.latency_min,
    avg_hr            = src.avg_hr,
    min_hr            = src.min_hr,
    max_hr            = src.max_hr,
    avg_hrv           = src.avg_hrv,
    lowest_hr         = src.lowest_hr,
    readiness_score   = src.readiness_score,
    snoring_min       = src.snoring_min,
    snoring_episodes  = src.snoring_episodes,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, date, bedtime_start, bedtime_end, total_sleep_min, deep_sleep_min, rem_sleep_min,
    light_sleep_min, awake_min, efficiency, latency_min, avg_hr, min_hr, max_hr, avg_hrv,
    lowest_hr, readiness_score, snoring_min, snoring_episodes,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.date, src.bedtime_start, src.bedtime_end, src.total_sleep_min, src.deep_sleep_min, src.rem_sleep_min,
    src.light_sleep_min, src.awake_min, src.efficiency, src.latency_min, src.avg_hr, src.min_hr, src.max_hr, src.avg_hrv,
    src.lowest_hr, src.readiness_score, src.snoring_min, src.snoring_episodes,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.sleep_session__withings_staging;
