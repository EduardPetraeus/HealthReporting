{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.audio_exposure.
-- Run once with: dbt run --select audio_exposure
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as date,
    null::varchar    as exposure_type,
    null::double     as avg_db,
    null::double     as max_db,
    null::integer    as sample_count,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
