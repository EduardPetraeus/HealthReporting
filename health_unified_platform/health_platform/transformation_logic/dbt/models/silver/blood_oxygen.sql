{{
   config(materialized='table')
}}

-- Schema-only definition for silver.blood_oxygen.
-- Run once with: dbt run --select blood_oxygen
-- Data is loaded by: dbt/merge/silver/merge_oura_blood_oxygen.sql

select
    null::integer    as sk_date,
    null::varchar    as sk_time,
    null::timestamp  as timestamp,
    null::double     as spo2,
    null::varchar    as source,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
