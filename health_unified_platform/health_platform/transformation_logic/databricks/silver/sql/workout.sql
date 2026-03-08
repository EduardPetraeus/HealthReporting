-- =============================================================================
-- workout.sql
-- Silver: Oura workout sessions
--
-- Source: health_dw.bronze.stg_oura_workout
--
-- Business key: business_key_hash (Oura workout UUID)
-- Change detection: row_hash over activity, calories, distance, intensity
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.workout (
    sk_date           INTEGER   NOT NULL,
    day               DATE      NOT NULL,
    workout_id        STRING,
    activity          STRING,
    intensity         STRING,
    calories          DOUBLE,
    distance_meters   DOUBLE,
    start_datetime    TIMESTAMP,
    end_datetime      TIMESTAMP,
    duration_seconds  DOUBLE,
    label             STRING,
    source            STRING,
    business_key_hash STRING    NOT NULL,
    row_hash          STRING    NOT NULL,
    load_datetime     TIMESTAMP NOT NULL,
    update_datetime   TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.workout_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM health_dw.bronze.stg_oura_workout
    WHERE id IS NOT NULL
)
SELECT
    year(to_date(day)) * 10000
        + month(to_date(day)) * 100
        + dayofmonth(to_date(day))                        AS sk_date,
    to_date(day)                                          AS day,
    id                                                    AS workout_id,
    activity,
    intensity,
    CAST(calories AS DOUBLE)                              AS calories,
    CAST(distance AS DOUBLE)                              AS distance_meters,
    to_timestamp(start_datetime)                          AS start_datetime,
    to_timestamp(end_datetime)                            AS end_datetime,
    CAST(
        unix_timestamp(to_timestamp(end_datetime))
        - unix_timestamp(to_timestamp(start_datetime))
        AS DOUBLE
    )                                                     AS duration_seconds,
    CAST(label AS STRING)                                 AS label,
    source,
    sha2(coalesce(id, ''), 256)                           AS business_key_hash,
    sha2(
        concat_ws('||',
            coalesce(id, ''),
            coalesce(activity, ''),
            coalesce(cast(calories AS STRING), ''),
            coalesce(cast(distance AS STRING), ''),
            coalesce(intensity, '')
        ), 256
    )                                                     AS row_hash,
    current_timestamp()                                   AS load_datetime
FROM deduped
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO health_dw.silver.workout AS target
USING health_dw.silver.workout_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.sk_date          = source.sk_date,
        target.day              = source.day,
        target.workout_id       = source.workout_id,
        target.activity         = source.activity,
        target.intensity        = source.intensity,
        target.calories         = source.calories,
        target.distance_meters  = source.distance_meters,
        target.start_datetime   = source.start_datetime,
        target.end_datetime     = source.end_datetime,
        target.duration_seconds = source.duration_seconds,
        target.label            = source.label,
        target.source           = source.source,
        target.row_hash         = source.row_hash,
        target.update_datetime  = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        sk_date, day, workout_id, activity, intensity, calories, distance_meters,
        start_datetime, end_datetime, duration_seconds, label, source,
        business_key_hash, row_hash, load_datetime, update_datetime
    )
    VALUES (
        source.sk_date, source.day, source.workout_id, source.activity,
        source.intensity, source.calories, source.distance_meters,
        source.start_datetime, source.end_datetime, source.duration_seconds,
        source.label, source.source,
        source.business_key_hash, source.row_hash,
        current_timestamp(), current_timestamp()
    );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.workout_staging;
