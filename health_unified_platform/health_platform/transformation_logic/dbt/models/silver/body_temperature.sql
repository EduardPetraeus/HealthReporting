{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.body_temperature.
-- Run once with: dbt run --select body_temperature
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::varchar    as sk_time,
    null::timestamp  as timestamp,
    null::double     as temperature_degc,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
