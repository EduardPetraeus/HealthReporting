-- merge_strava_activities.sql
-- Per-source merge: Strava API activities -> silver.workout (unified workout table)
-- Business key: activity_id || 'strava' (unique Strava activity ID per source)
-- Intensity derived from average heart rate: <120=easy, 120-150=moderate, >150=hard
--
-- Usage: python run_merge.py silver/merge_strava_activities.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.workout__staging_strava AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY activity_id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_strava_activities
    WHERE activity_id IS NOT NULL
)
SELECT
    -- Surrogate date key
    (year(start_date::TIMESTAMP) * 10000 + month(start_date::TIMESTAMP) * 100 + day(start_date::TIMESTAMP))::INTEGER AS sk_date,

    -- Business columns
    start_date::TIMESTAMP::DATE                         AS day,
    activity_id::VARCHAR                                AS workout_id,
    coalesce(sport_type, activity_type)::VARCHAR        AS activity,
    CASE
        WHEN average_heartrate::DOUBLE < 120 THEN 'easy'
        WHEN average_heartrate::DOUBLE BETWEEN 120 AND 150 THEN 'moderate'
        WHEN average_heartrate::DOUBLE > 150 THEN 'hard'
        ELSE NULL
    END                                                 AS intensity,
    coalesce(calories::DOUBLE, kilojoules::DOUBLE * 0.239006) AS calories,
    distance_m::DOUBLE                                  AS distance_meters,
    start_date::TIMESTAMP                               AS start_datetime,
    (start_date::TIMESTAMP + INTERVAL (elapsed_time_s::INTEGER) SECOND) AS end_datetime,
    elapsed_time_s::DOUBLE                              AS duration_seconds,
    name::VARCHAR                                       AS label,
    'strava'                                            AS source,

    -- Deterministic business key hash (includes source to avoid cross-source collisions)
    md5(coalesce(activity_id::VARCHAR, '') || '||strava') AS business_key_hash,

    -- Row hash (change detection)
    md5(
        coalesce(activity_id::VARCHAR, '')                      || '||' ||
        coalesce(coalesce(sport_type, activity_type), '')       || '||' ||
        coalesce(cast(calories AS VARCHAR), '')                 || '||' ||
        coalesce(cast(distance_m AS VARCHAR), '')               || '||' ||
        coalesce(cast(average_heartrate AS VARCHAR), '')        || '||' ||
        coalesce(cast(elapsed_time_s AS VARCHAR), '')           || '||' ||
        coalesce(name, '')
    ) AS row_hash,

    current_timestamp AS load_datetime

FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.workout
MERGE INTO silver.workout AS target
USING silver.workout__staging_strava AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date          = src.sk_date,
    day              = src.day,
    workout_id       = src.workout_id,
    activity         = src.activity,
    intensity        = src.intensity,
    calories         = src.calories,
    distance_meters  = src.distance_meters,
    start_datetime   = src.start_datetime,
    end_datetime     = src.end_datetime,
    duration_seconds = src.duration_seconds,
    label            = src.label,
    source           = src.source,
    row_hash         = src.row_hash,
    update_datetime  = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, day, workout_id, activity, intensity, calories, distance_meters,
    start_datetime, end_datetime, duration_seconds, label, source,
    business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.day, src.workout_id, src.activity, src.intensity, src.calories, src.distance_meters,
    src.start_datetime, src.end_datetime, src.duration_seconds, src.label, src.source,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.workout__staging_strava;
