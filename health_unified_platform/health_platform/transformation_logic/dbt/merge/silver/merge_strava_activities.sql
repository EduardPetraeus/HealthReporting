-- merge_strava_activities.sql
-- Per-source merge: Strava API -> silver.strava_activity
-- Business key: activity_id (unique per Strava activity)
--
-- Usage: python run_merge.py silver/merge_strava_activities.sql

CREATE OR REPLACE TABLE silver.strava_activity__staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY activity_id ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_strava_activities
    WHERE activity_id IS NOT NULL
)
SELECT
    (year(start_date::TIMESTAMP) * 10000 + month(start_date::TIMESTAMP) * 100 + day(start_date::TIMESTAMP))::INTEGER AS sk_date,
    start_date::TIMESTAMP                  AS start_date,
    start_date_local::TIMESTAMP            AS start_date_local,
    activity_id::BIGINT                    AS activity_id,
    name::VARCHAR                          AS name,
    activity_type::VARCHAR                 AS activity_type,
    sport_type::VARCHAR                    AS sport_type,
    distance_m::DOUBLE                     AS distance_m,
    moving_time_s::INTEGER                 AS duration_s,
    elapsed_time_s::INTEGER                AS elapsed_time_s,
    average_heartrate::DOUBLE              AS avg_hr,
    max_heartrate::DOUBLE                  AS max_hr,
    total_elevation_gain_m::DOUBLE         AS elevation_gain_m,
    average_watts::DOUBLE                  AS avg_power_w,
    max_watts::DOUBLE                      AS max_power_w,
    weighted_average_watts::DOUBLE         AS weighted_avg_power_w,
    average_speed_mps::DOUBLE              AS avg_speed_mps,
    max_speed_mps::DOUBLE                  AS max_speed_mps,
    average_cadence::DOUBLE                AS avg_cadence,
    average_temp::DOUBLE                   AS avg_temp_c,
    calories::DOUBLE                       AS calories,
    kilojoules::DOUBLE                     AS kilojoules,
    suffer_score::INTEGER                  AS suffer_score,
    has_heartrate::BOOLEAN                 AS has_heartrate,
    has_power::BOOLEAN                     AS has_power,
    md5(cast(activity_id AS VARCHAR))      AS business_key_hash,
    md5(
        coalesce(cast(activity_id AS VARCHAR), '')           || '||' ||
        coalesce(name, '')                                   || '||' ||
        coalesce(activity_type, '')                          || '||' ||
        coalesce(cast(distance_m AS VARCHAR), '')            || '||' ||
        coalesce(cast(moving_time_s AS VARCHAR), '')         || '||' ||
        coalesce(cast(average_heartrate AS VARCHAR), '')     || '||' ||
        coalesce(cast(max_heartrate AS VARCHAR), '')         || '||' ||
        coalesce(cast(total_elevation_gain_m AS VARCHAR), '') || '||' ||
        coalesce(cast(average_watts AS VARCHAR), '')         || '||' ||
        coalesce(cast(calories AS VARCHAR), '')
    )                                      AS row_hash,
    current_timestamp                      AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.strava_activity AS target
USING silver.strava_activity__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date              = src.sk_date,
    start_date           = src.start_date,
    start_date_local     = src.start_date_local,
    activity_id          = src.activity_id,
    name                 = src.name,
    activity_type        = src.activity_type,
    sport_type           = src.sport_type,
    distance_m           = src.distance_m,
    duration_s           = src.duration_s,
    elapsed_time_s       = src.elapsed_time_s,
    avg_hr               = src.avg_hr,
    max_hr               = src.max_hr,
    elevation_gain_m     = src.elevation_gain_m,
    avg_power_w          = src.avg_power_w,
    max_power_w          = src.max_power_w,
    weighted_avg_power_w = src.weighted_avg_power_w,
    avg_speed_mps        = src.avg_speed_mps,
    max_speed_mps        = src.max_speed_mps,
    avg_cadence          = src.avg_cadence,
    avg_temp_c           = src.avg_temp_c,
    calories             = src.calories,
    kilojoules           = src.kilojoules,
    suffer_score         = src.suffer_score,
    has_heartrate        = src.has_heartrate,
    has_power            = src.has_power,
    row_hash             = src.row_hash,
    update_datetime      = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, start_date, start_date_local, activity_id, name, activity_type,
    sport_type, distance_m, duration_s, elapsed_time_s, avg_hr, max_hr,
    elevation_gain_m, avg_power_w, max_power_w, weighted_avg_power_w,
    avg_speed_mps, max_speed_mps, avg_cadence, avg_temp_c, calories,
    kilojoules, suffer_score, has_heartrate, has_power,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.start_date, src.start_date_local, src.activity_id, src.name, src.activity_type,
    src.sport_type, src.distance_m, src.duration_s, src.elapsed_time_s, src.avg_hr, src.max_hr,
    src.elevation_gain_m, src.avg_power_w, src.max_power_w, src.weighted_avg_power_w,
    src.avg_speed_mps, src.max_speed_mps, src.avg_cadence, src.avg_temp_c, src.calories,
    src.kilojoules, src.suffer_score, src.has_heartrate, src.has_power,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.strava_activity__staging;
