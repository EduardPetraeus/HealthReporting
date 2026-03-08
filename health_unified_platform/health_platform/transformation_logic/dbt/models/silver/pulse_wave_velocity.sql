{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.pulse_wave_velocity.
-- Run once with: dbt run --select pulse_wave_velocity
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::timestamp  as timestamp,
    null::double     as pwv_m_per_s,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
