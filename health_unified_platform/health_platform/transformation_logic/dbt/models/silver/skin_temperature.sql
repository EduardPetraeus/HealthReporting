{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.skin_temperature.
-- Run once with: dbt run --select skin_temperature
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as date,
    null::double     as avg_skin_temp,
    null::double     as min_skin_temp,
    null::double     as max_skin_temp,
    null::integer    as sample_count,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
