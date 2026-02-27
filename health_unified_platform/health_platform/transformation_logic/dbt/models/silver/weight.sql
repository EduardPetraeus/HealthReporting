{{
   config(materialized='table')
}}

-- Schema-only definition for silver.weight.
-- Run once with: dbt run --select weight
-- Data is loaded by: dbt/merge/silver/merge_withings_weight.sql

select
    null::integer    as sk_date,
    null::varchar    as sk_time,
    null::timestamp  as datetime,
    null::double     as weight_kg,
    null::double     as fat_mass_kg,
    null::double     as bone_mass_kg,
    null::double     as muscle_mass_kg,
    null::double     as hydration_kg,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
