{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.ecg_session.
-- Run once with: dbt run --select ecg_session
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::timestamp  as timestamp,
    null::varchar    as ecg_type,
    null::integer    as frequency_hz,
    null::integer    as duration_s,
    null::varchar    as wearposition,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
