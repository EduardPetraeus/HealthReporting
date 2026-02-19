{{
    config(materialized='table')
}}

-- Schema-only definition for silver.workout.
-- Run once with: dbt run --select workout
-- Data is loaded by: dbt/merge/silver/merge_oura_workout.sql

select
    null::integer    as sk_date,
    null::date       as day,
    null::varchar    as workout_id,
    null::varchar    as activity,
    null::varchar    as intensity,
    null::double     as calories,
    null::double     as distance_meters,
    null::timestamp  as start_datetime,
    null::timestamp  as end_datetime,
    null::double     as duration_seconds,
    null::varchar    as label,
    null::varchar    as source,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
