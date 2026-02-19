-- merge_oura_workout.sql
-- Per-source merge: Oura API -> silver.workout
-- Business key: Oura workout UUID (id)
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
--
-- Usage: python run_merge.py silver/merge_oura_workout.sql

CREATE OR REPLACE TABLE silver.workout__staging AS
WITH deduped AS (
    SELECT *,
        make_date(year::INTEGER, month::VARCHAR::INTEGER, day::VARCHAR::INTEGER) AS full_date,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at_1 DESC) AS rn
    FROM bronze.stg_oura_workout
    WHERE id IS NOT NULL
)
SELECT
    (year::INTEGER * 10000 + month::VARCHAR::INTEGER * 100 + day::VARCHAR::INTEGER)::INTEGER AS sk_date,
    full_date                                          AS day,
    id                                                 AS workout_id,
    activity,
    intensity,
    calories::DOUBLE                                   AS calories,
    distance::DOUBLE                                   AS distance_meters,
    start_datetime::TIMESTAMP                          AS start_datetime,
    end_datetime::TIMESTAMP                            AS end_datetime,
    epoch_ms(end_datetime::TIMESTAMP) / 1000.0
        - epoch_ms(start_datetime::TIMESTAMP) / 1000.0 AS duration_seconds,
    label::VARCHAR                                     AS label,
    source,
    md5(coalesce(id, ''))                              AS business_key_hash,
    md5(
        coalesce(id, '')                                   || '||' ||
        coalesce(activity, '')                             || '||' ||
        coalesce(cast(calories AS VARCHAR), '')            || '||' ||
        coalesce(cast(distance AS VARCHAR), '')            || '||' ||
        coalesce(intensity, '')
    )                                                  AS row_hash,
    current_timestamp                                  AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.workout AS target
USING silver.workout__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
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

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, workout_id, activity, intensity, calories, distance_meters,
    start_datetime, end_datetime, duration_seconds, label, source,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.workout_id, src.activity, src.intensity, src.calories, src.distance_meters,
    src.start_datetime, src.end_datetime, src.duration_seconds, src.label, src.source,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.workout__staging;
