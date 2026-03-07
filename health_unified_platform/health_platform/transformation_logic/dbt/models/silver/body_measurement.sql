{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.body_measurement.
-- Run once with: dbt run --select body_measurement
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as date,
    null::double     as weight_kg,
    null::double     as body_fat_pct,
    null::double     as lean_body_mass_kg,
    null::double     as bmi,
    null::double     as height_m,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
