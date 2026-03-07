{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.physical_effort.
-- Run once with: dbt run --select physical_effort
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::varchar    as sk_time,
    null::timestamp  as timestamp,
    null::double     as effort_kj_per_hr_kg,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
