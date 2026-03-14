-- merge_withings_sleep_session.sql
-- Per-source merge: Withings API + CSV -> silver.sleep_session (shared table)
-- UNION ALL from API (stg_withings_sleep_summary) and CSV (stg_withings_sleep_csv)
-- Source-qualified staging table to avoid collisions with Oura merge.
--
-- Usage: python run_merge.py silver/merge_withings_sleep_session.sql

-- Step 1: Create staging table with UNION ALL from both sources
CREATE OR REPLACE TABLE silver.sleep_session__withings_staging AS
WITH api_source AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY startdate ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_sleep_summary
    WHERE startdate IS NOT NULL
),
csv_source AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY "from" ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_sleep_csv
    WHERE "from" IS NOT NULL
),
combined AS (
    -- API sleep summary: durations already in seconds
    SELECT
        CASE
            WHEN startdate ~ '^\d+$' THEN epoch_ms(startdate::BIGINT * 1000)::TIMESTAMP
            ELSE startdate::TIMESTAMP
        END AS bedtime_start,
        CASE
            WHEN enddate ~ '^\d+$' THEN epoch_ms(enddate::BIGINT * 1000)::TIMESTAMP
            ELSE enddate::TIMESTAMP
        END AS bedtime_end,
        ROUND((TRY_CAST(lightsleepduration AS DOUBLE) + TRY_CAST(deepsleepduration AS DOUBLE) + TRY_CAST(remsleepduration AS DOUBLE)) / 60.0, 1) AS total_sleep_min,
        ROUND(TRY_CAST(deepsleepduration AS DOUBLE) / 60.0, 1) AS deep_sleep_min,
        ROUND(TRY_CAST(remsleepduration AS DOUBLE) / 60.0, 1) AS rem_sleep_min,
        ROUND(TRY_CAST(lightsleepduration AS DOUBLE) / 60.0, 1) AS light_sleep_min,
        ROUND(TRY_CAST(wakeupduration AS DOUBLE) / 60.0, 1) AS awake_min,
        TRY_CAST(sleep_score AS INTEGER) AS efficiency,
        ROUND(TRY_CAST(durationtosleep AS DOUBLE) / 60.0, 1) AS latency_min,
        ROUND(TRY_CAST(hr_average AS DOUBLE), 1) AS avg_hr,
        TRY_CAST(hr_min AS INTEGER) AS min_hr,
        TRY_CAST(hr_max AS INTEGER) AS max_hr,
        NULL::DOUBLE AS avg_hrv,
        TRY_CAST(hr_min AS INTEGER) AS lowest_hr,
        TRY_CAST(sleep_score AS INTEGER) AS readiness_score,
        ROUND(TRY_CAST(snoring AS DOUBLE) / 60.0, 1) AS snoring_min,
        TRY_CAST(snoringepisodecount AS INTEGER) AS snoring_episodes,
        'withings' AS source_name
    FROM api_source WHERE rn = 1

    UNION ALL

    -- CSV sleep export
    SELECT
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
        'withings' AS source_name
    FROM csv_source WHERE rn = 1
),
final_dedup AS (
    SELECT *,
        bedtime_start::DATE AS date,
        ROW_NUMBER() OVER (PARTITION BY bedtime_start ORDER BY total_sleep_min DESC NULLS LAST) AS rn
    FROM combined
    WHERE bedtime_start IS NOT NULL
)
SELECT
    (year(date) * 10000 + month(date) * 100 + day(date))::INTEGER AS sk_date,
    date,
    bedtime_start,
    bedtime_end,
    total_sleep_min,
    deep_sleep_min,
    rem_sleep_min,
    light_sleep_min,
    awake_min,
    efficiency,
    latency_min,
    avg_hr,
    min_hr,
    max_hr,
    avg_hrv,
    lowest_hr,
    readiness_score,
    snoring_min,
    snoring_episodes,
    source_name,
    md5(
        coalesce(cast(bedtime_start AS VARCHAR), '') || '||' || 'withings'
    ) AS business_key_hash,
    md5(
        coalesce(cast(bedtime_start AS VARCHAR), '') || '||' ||
        coalesce(cast(bedtime_end AS VARCHAR), '') || '||' ||
        coalesce(cast(total_sleep_min AS VARCHAR), '') || '||' ||
        coalesce(cast(deep_sleep_min AS VARCHAR), '') || '||' ||
        coalesce(cast(avg_hr AS VARCHAR), '') || '||' ||
        coalesce(cast(snoring_min AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM final_dedup WHERE rn = 1;

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
