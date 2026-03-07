{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.blood_pressure_v2.
-- Run once with: dbt run --select blood_pressure_v2
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.
-- Note: replaces silver.blood_pressure (Withings-only) in A7.

select
    null::integer    as sk_date,
    null::varchar    as sk_time,
    null::timestamp  as timestamp,
    null::double     as systolic_mmhg,
    null::double     as diastolic_mmhg,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
