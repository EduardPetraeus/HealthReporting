{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.sleep_session.
-- Run once with: dbt run --select sleep_session
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as date,
    null::timestamp  as bedtime_start,
    null::timestamp  as bedtime_end,
    null::double     as total_sleep_min,
    null::double     as deep_sleep_min,
    null::double     as rem_sleep_min,
    null::double     as light_sleep_min,
    null::double     as awake_min,
    null::integer    as efficiency,
    null::double     as latency_min,
    null::double     as avg_hr,
    null::integer    as min_hr,
    null::integer    as max_hr,
    null::double     as avg_hrv,
    null::integer    as lowest_hr,
    null::integer    as readiness_score,
    null::double     as snoring_min,
    null::integer    as snoring_episodes,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
