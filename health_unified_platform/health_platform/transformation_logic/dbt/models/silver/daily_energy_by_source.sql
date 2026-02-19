{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.daily_energy_by_source.
-- Run once with: dbt run --select daily_energy_by_source
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as date,
    null::varchar    as source_name,
    null::double     as active_energy_kcal,
    null::double     as basal_energy_kcal,
    null::double     as total_energy_kcal,
    null::integer    as active_sessions,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
