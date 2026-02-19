{{
    config(materialized='table')
}}

-- Schema-only definition for silver.personal_info.
-- Run once with: dbt run --select personal_info
-- Data is loaded by: dbt/merge/silver/merge_oura_personal_info.sql

select
    null::integer    as age,
    null::double     as weight_kg,
    null::double     as height_m,
    null::varchar    as biological_sex,
    null::varchar    as email,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
