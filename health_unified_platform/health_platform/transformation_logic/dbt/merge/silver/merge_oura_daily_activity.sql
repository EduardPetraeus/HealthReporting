-- merge_oura_daily_activity.sql
-- Per-source merge: Oura API -> silver.daily_activity
-- Business key: day (one row per day)
-- Note: met and class_5_min excluded intentionally.
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
--
-- Usage: python run_merge.py silver/merge_oura_daily_activity.sql

CREATE OR REPLACE TABLE silver.daily_activity__staging AS
WITH deduped AS (
    SELECT *,
        make_date(year::INTEGER, month::VARCHAR::INTEGER, day::VARCHAR::INTEGER) AS full_date,
        ROW_NUMBER() OVER (
            PARTITION BY year, month, day
            ORDER BY _ingested_at_1 DESC
        ) AS rn
    FROM bronze.stg_oura_daily_activity
    WHERE day IS NOT NULL
)
SELECT
    (year::INTEGER * 10000 + month::VARCHAR::INTEGER * 100 + day::VARCHAR::INTEGER)::INTEGER AS sk_date,
    full_date                                         AS day,
    score::INTEGER                                    AS activity_score,
    timestamp::TIMESTAMP                              AS timestamp,
    steps::INTEGER                                    AS steps,
    total_calories::INTEGER                           AS total_calories,
    active_calories::INTEGER                          AS active_calories,
    target_calories::INTEGER                          AS target_calories,
    average_met_minutes::DOUBLE                       AS average_met_minutes,
    equivalent_walking_distance::INTEGER              AS equivalent_walking_distance,
    high_activity_time::INTEGER                       AS high_activity_time,
    medium_activity_time::INTEGER                     AS medium_activity_time,
    low_activity_time::INTEGER                        AS low_activity_time,
    sedentary_time::INTEGER                           AS sedentary_time,
    resting_time::INTEGER                             AS resting_time,
    non_wear_time::INTEGER                            AS non_wear_time,
    high_activity_met_minutes::INTEGER                AS high_activity_met_minutes,
    medium_activity_met_minutes::INTEGER              AS medium_activity_met_minutes,
    low_activity_met_minutes::INTEGER                 AS low_activity_met_minutes,
    sedentary_met_minutes::INTEGER                    AS sedentary_met_minutes,
    inactivity_alerts::INTEGER                        AS inactivity_alerts,
    target_meters::INTEGER                            AS target_meters,
    meters_to_target::INTEGER                         AS meters_to_target,
    contributors.meet_daily_targets::INTEGER          AS contributor_meet_daily_targets,
    contributors.move_every_hour::INTEGER             AS contributor_move_every_hour,
    contributors.recovery_time::INTEGER               AS contributor_recovery_time,
    contributors.stay_active::INTEGER                 AS contributor_stay_active,
    contributors.training_frequency::INTEGER          AS contributor_training_frequency,
    contributors.training_volume::INTEGER             AS contributor_training_volume,
    md5(full_date::VARCHAR)                           AS business_key_hash,
    md5(
        coalesce(cast(score AS VARCHAR), '')                              || '||' ||
        coalesce(cast(steps AS VARCHAR), '')                              || '||' ||
        coalesce(cast(total_calories AS VARCHAR), '')                     || '||' ||
        coalesce(cast(active_calories AS VARCHAR), '')                    || '||' ||
        coalesce(cast(contributors.meet_daily_targets AS VARCHAR), '')    || '||' ||
        coalesce(cast(contributors.move_every_hour AS VARCHAR), '')       || '||' ||
        coalesce(cast(contributors.recovery_time AS VARCHAR), '')         || '||' ||
        coalesce(cast(contributors.stay_active AS VARCHAR), '')           || '||' ||
        coalesce(cast(contributors.training_frequency AS VARCHAR), '')    || '||' ||
        coalesce(cast(contributors.training_volume AS VARCHAR), '')
    )                                                 AS row_hash,
    current_timestamp                                 AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.daily_activity AS target
USING silver.daily_activity__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date                        = src.sk_date,
    day                            = src.day,
    activity_score                 = src.activity_score,
    timestamp                      = src.timestamp,
    steps                          = src.steps,
    total_calories                 = src.total_calories,
    active_calories                = src.active_calories,
    target_calories                = src.target_calories,
    average_met_minutes            = src.average_met_minutes,
    equivalent_walking_distance    = src.equivalent_walking_distance,
    high_activity_time             = src.high_activity_time,
    medium_activity_time           = src.medium_activity_time,
    low_activity_time              = src.low_activity_time,
    sedentary_time                 = src.sedentary_time,
    resting_time                   = src.resting_time,
    non_wear_time                  = src.non_wear_time,
    high_activity_met_minutes      = src.high_activity_met_minutes,
    medium_activity_met_minutes    = src.medium_activity_met_minutes,
    low_activity_met_minutes       = src.low_activity_met_minutes,
    sedentary_met_minutes          = src.sedentary_met_minutes,
    inactivity_alerts              = src.inactivity_alerts,
    target_meters                  = src.target_meters,
    meters_to_target               = src.meters_to_target,
    contributor_meet_daily_targets = src.contributor_meet_daily_targets,
    contributor_move_every_hour    = src.contributor_move_every_hour,
    contributor_recovery_time      = src.contributor_recovery_time,
    contributor_stay_active        = src.contributor_stay_active,
    contributor_training_frequency = src.contributor_training_frequency,
    contributor_training_volume    = src.contributor_training_volume,
    row_hash                       = src.row_hash,
    update_datetime                = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, activity_score, timestamp, steps, total_calories, active_calories,
    target_calories, average_met_minutes, equivalent_walking_distance,
    high_activity_time, medium_activity_time, low_activity_time,
    sedentary_time, resting_time, non_wear_time,
    high_activity_met_minutes, medium_activity_met_minutes,
    low_activity_met_minutes, sedentary_met_minutes,
    inactivity_alerts, target_meters, meters_to_target,
    contributor_meet_daily_targets, contributor_move_every_hour,
    contributor_recovery_time, contributor_stay_active,
    contributor_training_frequency, contributor_training_volume,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.activity_score, src.timestamp, src.steps, src.total_calories, src.active_calories,
    src.target_calories, src.average_met_minutes, src.equivalent_walking_distance,
    src.high_activity_time, src.medium_activity_time, src.low_activity_time,
    src.sedentary_time, src.resting_time, src.non_wear_time,
    src.high_activity_met_minutes, src.medium_activity_met_minutes,
    src.low_activity_met_minutes, src.sedentary_met_minutes,
    src.inactivity_alerts, src.target_meters, src.meters_to_target,
    src.contributor_meet_daily_targets, src.contributor_move_every_hour,
    src.contributor_recovery_time, src.contributor_stay_active,
    src.contributor_training_frequency, src.contributor_training_volume,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.daily_activity__staging;
