{{
    config(materialized='table')
}}

-- Schema-only definition for silver.daily_activity.
-- Run once with: dbt run --select daily_activity
-- Data is loaded by: dbt/merge/silver/merge_oura_daily_activity.sql

select
    null::integer    as sk_date,
    null::date       as day,
    null::integer    as activity_score,
    null::timestamp  as timestamp,
    null::integer    as steps,
    null::integer    as total_calories,
    null::integer    as active_calories,
    null::integer    as target_calories,
    null::double     as average_met_minutes,
    null::integer    as equivalent_walking_distance,
    null::integer    as high_activity_time,
    null::integer    as medium_activity_time,
    null::integer    as low_activity_time,
    null::integer    as sedentary_time,
    null::integer    as resting_time,
    null::integer    as non_wear_time,
    null::integer    as high_activity_met_minutes,
    null::integer    as medium_activity_met_minutes,
    null::integer    as low_activity_met_minutes,
    null::integer    as sedentary_met_minutes,
    null::integer    as inactivity_alerts,
    null::integer    as target_meters,
    null::integer    as meters_to_target,
    null::integer    as contributor_meet_daily_targets,
    null::integer    as contributor_move_every_hour,
    null::integer    as contributor_recovery_time,
    null::integer    as contributor_stay_active,
    null::integer    as contributor_training_frequency,
    null::integer    as contributor_training_volume,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
