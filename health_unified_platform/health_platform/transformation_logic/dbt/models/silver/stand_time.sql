{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.stand_time.
-- Run once with: dbt run --select stand_time
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as date,
    null::double     as stand_minutes,
    null::integer    as stand_hours,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
