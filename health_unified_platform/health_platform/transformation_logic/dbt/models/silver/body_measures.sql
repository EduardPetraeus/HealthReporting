{{
    config(materialized='table')
}}

-- Schema-only definition for silver.body_measures.
-- Run once with: dbt run --select body_measures
-- Data is loaded by: dbt/merge/silver/merge_lifesum_bodymeasures.sql

select
    null::integer    as sk_date,
    null::date       as day,
    null::double     as weight_kg,
    null::double     as body_fat_pct,
    null::double     as muscle_mass_pct,
    null::double     as waist_cm,
    null::double     as hip_cm,
    null::double     as chest_cm,
    null::varchar    as source_system,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
