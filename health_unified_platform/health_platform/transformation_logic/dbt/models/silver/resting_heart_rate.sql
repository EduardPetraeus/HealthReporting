{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.resting_heart_rate.
-- Run once with: dbt run --select resting_heart_rate
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as date,
    null::double     as resting_hr_bpm,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
