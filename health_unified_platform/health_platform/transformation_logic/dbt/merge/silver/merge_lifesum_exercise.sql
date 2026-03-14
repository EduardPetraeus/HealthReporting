-- merge_lifesum_exercise.sql
-- Per-source merge: Lifesum exercise CSV export -> silver.workout
-- Business key: date || title || 'lifesum' (unique per exercise per day per source)
--
-- Usage: python run_merge.py silver/merge_lifesum_exercise.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.workout__staging_lifesum AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date, title
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_lifesum_exercise
    WHERE date IS NOT NULL
      AND title IS NOT NULL
)
SELECT
    -- Surrogate date key
    (year(date::DATE) * 10000 + month(date::DATE) * 100 + day(date::DATE))::INTEGER AS sk_date,

    -- Business columns
    date::DATE                                          AS day,
    md5(coalesce(date, '') || '||' || coalesce(title, '')) AS workout_id,
    title::VARCHAR                                      AS activity,
    NULL::VARCHAR                                       AS intensity,
    calories_burned::DOUBLE                             AS calories,
    NULL::DOUBLE                                        AS distance_meters,
    date::TIMESTAMP                                     AS start_datetime,
    (date::TIMESTAMP + INTERVAL (duration_min::INTEGER) MINUTE) AS end_datetime,
    (duration_min::DOUBLE * 60)                         AS duration_seconds,
    title::VARCHAR                                      AS label,
    'lifesum'                                           AS source,

    -- Deterministic business key hash (includes source to avoid cross-source collisions)
    md5(coalesce(date, '') || '||' || coalesce(title, '') || '||lifesum') AS business_key_hash,

    -- Row hash (change detection)
    md5(
        coalesce(title, '')                                       || '||' ||
        coalesce(cast(duration_min AS VARCHAR), '')                || '||' ||
        coalesce(cast(calories_burned AS VARCHAR), '')             || '||' ||
        coalesce(source, '')
    ) AS row_hash,

    current_timestamp AS load_datetime

FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.workout
MERGE INTO silver.workout AS target
USING silver.workout__staging_lifesum AS src
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
DROP TABLE IF EXISTS silver.workout__staging_lifesum;
