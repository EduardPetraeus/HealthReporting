{{
   config(materialized='table')
}}

-- Schema-only definition for silver.blood_pressure.
-- Run once with: dbt run --select blood_pressure
-- Data is loaded by: dbt/merge/silver/merge_withings_blood_pressure.sql

select
    null::integer    as sk_date,
    null::varchar    as sk_time,
    null::timestamp  as datetime,
    null::integer    as systolic,
    null::integer    as diastolic,
    null::integer    as pulse,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
