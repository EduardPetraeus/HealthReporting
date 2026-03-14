-- merge_withings_workouts.sql
-- Per-source merge: Withings API -> silver.workout
-- Workout sessions with category, duration, HR, calories, distance.
-- Source-qualified staging table to avoid collisions with other source merges.
--
-- Usage: python run_merge.py silver/merge_withings_workouts.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.workout__withings_staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_workouts
    WHERE startdate IS NOT NULL
)
SELECT
    (year(
        CASE WHEN startdate ~ '^\d+$' THEN epoch_ms(startdate::BIGINT * 1000)::TIMESTAMP
             ELSE startdate::TIMESTAMP END
    ::DATE) * 10000 + month(
        CASE WHEN startdate ~ '^\d+$' THEN epoch_ms(startdate::BIGINT * 1000)::TIMESTAMP
             ELSE startdate::TIMESTAMP END
    ::DATE) * 100 + day(
        CASE WHEN startdate ~ '^\d+$' THEN epoch_ms(startdate::BIGINT * 1000)::TIMESTAMP
             ELSE startdate::TIMESTAMP END
    ::DATE))::INTEGER AS sk_date,
    CASE WHEN startdate ~ '^\d+$' THEN epoch_ms(startdate::BIGINT * 1000)::TIMESTAMP
         ELSE startdate::TIMESTAMP END AS start_datetime,
    CASE WHEN enddate ~ '^\d+$' THEN epoch_ms(enddate::BIGINT * 1000)::TIMESTAMP
         ELSE enddate::TIMESTAMP END AS end_datetime,
    TRY_CAST(category AS INTEGER) AS category,
    TRY_CAST(calories AS INTEGER) AS calories,
    TRY_CAST(steps AS INTEGER) AS steps,
    ROUND(TRY_CAST(distance AS DOUBLE), 1) AS distance_m,
    ROUND(TRY_CAST(elevation AS DOUBLE), 1) AS elevation_m,
    TRY_CAST(intensity AS INTEGER) AS intensity,
    ROUND(TRY_CAST(hr_average AS DOUBLE), 1) AS hr_average,
    TRY_CAST(hr_min AS INTEGER) AS hr_min,
    TRY_CAST(hr_max AS INTEGER) AS hr_max,
    TRY_CAST(hr_zone_0 AS INTEGER) AS hr_zone_0_s,
    TRY_CAST(hr_zone_1 AS INTEGER) AS hr_zone_1_s,
    TRY_CAST(hr_zone_2 AS INTEGER) AS hr_zone_2_s,
    TRY_CAST(hr_zone_3 AS INTEGER) AS hr_zone_3_s,
    TRY_CAST(pause_duration AS INTEGER) AS pause_duration_s,
    ROUND(TRY_CAST(spo2_average AS DOUBLE), 1) AS spo2_average,
    'withings' AS source_name,
    md5(
        coalesce(cast(id AS VARCHAR), '') || '||' || 'withings'
    ) AS business_key_hash,
    md5(
        coalesce(cast(id AS VARCHAR), '') || '||' ||
        coalesce(cast(category AS VARCHAR), '') || '||' ||
        coalesce(cast(calories AS VARCHAR), '') || '||' ||
        coalesce(cast(steps AS VARCHAR), '') || '||' ||
        coalesce(cast(hr_average AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped WHERE rn = 1;

-- Step 2: Merge staging into silver.workout
MERGE INTO silver.workout AS target
USING silver.workout__withings_staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date          = src.sk_date,
    start_datetime   = src.start_datetime,
    end_datetime     = src.end_datetime,
    category         = src.category,
    calories         = src.calories,
    steps            = src.steps,
    distance_m       = src.distance_m,
    elevation_m      = src.elevation_m,
    intensity        = src.intensity,
    hr_average       = src.hr_average,
    hr_min           = src.hr_min,
    hr_max           = src.hr_max,
    hr_zone_0_s      = src.hr_zone_0_s,
    hr_zone_1_s      = src.hr_zone_1_s,
    hr_zone_2_s      = src.hr_zone_2_s,
    hr_zone_3_s      = src.hr_zone_3_s,
    pause_duration_s = src.pause_duration_s,
    spo2_average     = src.spo2_average,
    source_name      = src.source_name,
    row_hash         = src.row_hash,
    update_datetime  = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, start_datetime, end_datetime, category, calories, steps, distance_m, elevation_m,
    intensity, hr_average, hr_min, hr_max, hr_zone_0_s, hr_zone_1_s, hr_zone_2_s, hr_zone_3_s,
    pause_duration_s, spo2_average,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.start_datetime, src.end_datetime, src.category, src.calories, src.steps, src.distance_m, src.elevation_m,
    src.intensity, src.hr_average, src.hr_min, src.hr_max, src.hr_zone_0_s, src.hr_zone_1_s, src.hr_zone_2_s, src.hr_zone_3_s,
    src.pause_duration_s, src.spo2_average,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.workout__withings_staging;
