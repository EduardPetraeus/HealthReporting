{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.daily_walking_gait.
-- Run once with: dbt run --select daily_walking_gait
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as date,
    null::double     as walking_speed_avg_km_hr,
    null::double     as walking_step_length_avg_cm,
    null::double     as walking_asymmetry_avg_pct,
    null::double     as walking_double_support_avg_pct,
    null::double     as walking_heart_rate_avg,
    null::double     as walking_steadiness_pct,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
