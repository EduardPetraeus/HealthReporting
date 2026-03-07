{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.hrv.
-- Run once with: dbt run --select hrv
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::varchar    as sk_time,
    null::timestamp  as timestamp,
    null::double     as hrv_ms,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
