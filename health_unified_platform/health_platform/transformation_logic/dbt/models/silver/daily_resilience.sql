{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.daily_resilience.
-- Run once with: dbt run --select daily_resilience
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as date,
    null::double     as sleep_recovery,
    null::double     as daytime_recovery,
    null::double     as stress,
    null::varchar    as level,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
