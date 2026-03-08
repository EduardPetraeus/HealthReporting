-- merge_oura_csv_sleep_session.sql
-- Per-source merge: Oura CSV -> silver.sleep_session
-- Shared table: source_name distinguishes Oura from Withings sleep data.
-- Filters out 'rest' type entries (naps without full sleep metrics).
--
-- Usage: python run_merge.py silver/merge_oura_csv_sleep_session.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.sleep_session__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY day, bedtime_start
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_oura_csv_sleepmodel
    WHERE day IS NOT NULL
      AND type IS NOT NULL
      AND type != 'rest'
)
SELECT
    (year(day::DATE) * 10000 + month(day::DATE) * 100 + day(day::DATE))::INTEGER AS sk_date,
    day::DATE AS date,
    TRY_CAST(bedtime_start AS TIMESTAMP) AS bedtime_start,
    TRY_CAST(bedtime_end AS TIMESTAMP) AS bedtime_end,
    ROUND(TRY_CAST(total_sleep_duration AS DOUBLE) / 60.0, 1) AS total_sleep_min,
    ROUND(TRY_CAST(deep_sleep_duration AS DOUBLE) / 60.0, 1) AS deep_sleep_min,
    ROUND(TRY_CAST(rem_sleep_duration AS DOUBLE) / 60.0, 1) AS rem_sleep_min,
    ROUND(TRY_CAST(light_sleep_duration AS DOUBLE) / 60.0, 1) AS light_sleep_min,
    ROUND(TRY_CAST(awake_time AS DOUBLE) / 60.0, 1) AS awake_min,
    TRY_CAST(efficiency AS INTEGER) AS efficiency,
    ROUND(TRY_CAST(latency AS DOUBLE) / 60.0, 1) AS latency_min,
    ROUND(TRY_CAST(average_heart_rate AS DOUBLE), 1) AS avg_hr,
    TRY_CAST(lowest_heart_rate AS INTEGER) AS min_hr,
    NULL::INTEGER AS max_hr,
    ROUND(TRY_CAST(average_hrv AS DOUBLE), 1) AS avg_hrv,
    TRY_CAST(lowest_heart_rate AS INTEGER) AS lowest_hr,
    CASE WHEN readiness IS NOT NULL AND readiness != ''
         THEN TRY_CAST(json_extract_string(readiness, '$.score') AS INTEGER)
         ELSE NULL END AS readiness_score,
    NULL::DOUBLE AS snoring_min,
    NULL::INTEGER AS snoring_episodes,
    'oura' AS source_name,
    md5(
        coalesce(cast(day AS VARCHAR), '') || '||' ||
        coalesce(cast(bedtime_start AS VARCHAR), '') || '||' || 'oura'
    ) AS business_key_hash,
    md5(
        coalesce(cast(day AS VARCHAR), '') || '||' ||
        coalesce(cast(total_sleep_duration AS VARCHAR), '') || '||' ||
        coalesce(cast(deep_sleep_duration AS VARCHAR), '') || '||' ||
        coalesce(cast(efficiency AS VARCHAR), '') || '||' ||
        coalesce(cast(average_heart_rate AS VARCHAR), '') || '||' ||
        coalesce(cast(average_hrv AS VARCHAR), '') || '||' ||
        coalesce(cast(bedtime_start AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.sleep_session
MERGE INTO silver.sleep_session AS target
USING silver.sleep_session__staging AS src
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
DROP TABLE IF EXISTS silver.sleep_session__staging;
