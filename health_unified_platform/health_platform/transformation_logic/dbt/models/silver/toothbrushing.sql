{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.toothbrushing.
-- Run once with: dbt run --select toothbrushing
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::varchar    as sk_time,
    null::timestamp  as timestamp,
    null::timestamp  as end_timestamp,
    null::double     as duration_seconds,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
