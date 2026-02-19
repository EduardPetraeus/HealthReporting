{{
    config(materialized='table')
}}

-- Schema-only definition for silver.daily_readiness.
-- Run once with: dbt run --select daily_readiness
-- Data is loaded by: dbt/merge/silver/merge_oura_daily_readiness.sql

select
    null::integer    as sk_date,
    null::date       as day,
    null::integer    as readiness_score,
    null::timestamp  as timestamp,
    null::double     as temperature_deviation,
    null::double     as temperature_trend_deviation,
    null::integer    as contributor_activity_balance,
    null::integer    as contributor_body_temperature,
    null::integer    as contributor_hrv_balance,
    null::integer    as contributor_previous_day_activity,
    null::integer    as contributor_previous_night,
    null::integer    as contributor_recovery_index,
    null::integer    as contributor_resting_heart_rate,
    null::integer    as contributor_sleep_balance,
    null::integer    as contributor_sleep_regularity,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
