{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.vo2_max.
-- Run once with: dbt run --select vo2_max
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as date,
    null::double     as vo2_max_ml_kg_min,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
