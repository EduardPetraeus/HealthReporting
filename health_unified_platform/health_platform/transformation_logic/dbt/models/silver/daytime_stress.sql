{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.daytime_stress.
-- Run once with: dbt run --select daytime_stress
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as date,
    null::double     as avg_stress,
    null::double     as max_stress,
    null::double     as avg_recovery,
    null::double     as max_recovery,
    null::integer    as sample_count,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
