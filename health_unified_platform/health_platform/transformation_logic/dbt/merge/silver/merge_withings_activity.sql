-- merge_withings_activity.sql
-- Per-source merge: Withings API -> silver.daily_activity
-- Daily aggregated activity: steps, distance, elevation, calories, HR zones.
-- Source-qualified staging table to avoid collisions with other source merges.
--
-- Usage: python run_merge.py silver/merge_withings_activity.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.daily_activity__withings_staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_activity
    WHERE date IS NOT NULL
)
SELECT
    (year(date::DATE) * 10000 + month(date::DATE) * 100 + day(date::DATE))::INTEGER AS sk_date,
    date::DATE AS date,
    TRY_CAST(steps AS INTEGER) AS steps,
    ROUND(TRY_CAST(distance AS DOUBLE), 1) AS distance_m,
    ROUND(TRY_CAST(elevation AS DOUBLE), 1) AS elevation_m,
    TRY_CAST(calories AS INTEGER) AS calories,
    TRY_CAST(active_calories AS INTEGER) AS active_calories,
    TRY_CAST(total_calories AS INTEGER) AS total_calories,
    TRY_CAST(soft_activity_duration AS INTEGER) AS soft_activity_duration_s,
    TRY_CAST(moderate_activity_duration AS INTEGER) AS moderate_activity_duration_s,
    TRY_CAST(intense_activity_duration AS INTEGER) AS intense_activity_duration_s,
    ROUND(TRY_CAST(hr_average AS DOUBLE), 1) AS hr_average,
    TRY_CAST(hr_min AS INTEGER) AS hr_min,
    TRY_CAST(hr_max AS INTEGER) AS hr_max,
    TRY_CAST(hr_zone_0 AS INTEGER) AS hr_zone_0_s,
    TRY_CAST(hr_zone_1 AS INTEGER) AS hr_zone_1_s,
    TRY_CAST(hr_zone_2 AS INTEGER) AS hr_zone_2_s,
    TRY_CAST(hr_zone_3 AS INTEGER) AS hr_zone_3_s,
    'withings' AS source_name,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' || 'withings'
    ) AS business_key_hash,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' ||
        coalesce(cast(steps AS VARCHAR), '') || '||' ||
        coalesce(cast(distance AS VARCHAR), '') || '||' ||
        coalesce(cast(calories AS VARCHAR), '') || '||' ||
        coalesce(cast(hr_average AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped WHERE rn = 1;

-- Step 2: Merge staging into silver.daily_activity
MERGE INTO silver.daily_activity AS target
USING silver.daily_activity__withings_staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date                      = src.sk_date,
    date                         = src.date,
    steps                        = src.steps,
    distance_m                   = src.distance_m,
    elevation_m                  = src.elevation_m,
    calories                     = src.calories,
    active_calories              = src.active_calories,
    total_calories               = src.total_calories,
    soft_activity_duration_s     = src.soft_activity_duration_s,
    moderate_activity_duration_s = src.moderate_activity_duration_s,
    intense_activity_duration_s  = src.intense_activity_duration_s,
    hr_average                   = src.hr_average,
    hr_min                       = src.hr_min,
    hr_max                       = src.hr_max,
    hr_zone_0_s                  = src.hr_zone_0_s,
    hr_zone_1_s                  = src.hr_zone_1_s,
    hr_zone_2_s                  = src.hr_zone_2_s,
    hr_zone_3_s                  = src.hr_zone_3_s,
    source_name                  = src.source_name,
    row_hash                     = src.row_hash,
    update_datetime              = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, date, steps, distance_m, elevation_m, calories, active_calories, total_calories,
    soft_activity_duration_s, moderate_activity_duration_s, intense_activity_duration_s,
    hr_average, hr_min, hr_max, hr_zone_0_s, hr_zone_1_s, hr_zone_2_s, hr_zone_3_s,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.date, src.steps, src.distance_m, src.elevation_m, src.calories, src.active_calories, src.total_calories,
    src.soft_activity_duration_s, src.moderate_activity_duration_s, src.intense_activity_duration_s,
    src.hr_average, src.hr_min, src.hr_max, src.hr_zone_0_s, src.hr_zone_1_s, src.hr_zone_2_s, src.hr_zone_3_s,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.daily_activity__withings_staging;
