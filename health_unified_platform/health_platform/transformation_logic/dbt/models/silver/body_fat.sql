{{
    config(materialized='table')
}}

-- Schema-only definition for silver.body_fat.
-- Run once with: dbt run --select body_fat
-- Data is loaded by: dbt/merge/silver/merge_lifesum_bodyfat.sql

select
    null::integer    as sk_date,
    null::date       as day,
    null::double     as body_fat_pct,
    null::varchar    as source_system,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
