{{
   config(materialized='table')
}}

-- Schema-only definition for silver.lab_results.
-- Run once with: dbt run --select lab_results
-- Data is loaded by: ingestion/import_manual_data.py

select
    null::varchar    as test_id,
    null::date       as test_date,
    null::varchar    as test_type,
    null::varchar    as test_name,
    null::varchar    as lab_name,
    null::varchar    as lab_accreditation,
    null::varchar    as marker_name,
    null::varchar    as marker_category,
    null::double     as value_numeric,
    null::varchar    as value_text,
    null::varchar    as unit,
    null::double     as reference_min,
    null::double     as reference_max,
    null::varchar    as reference_direction,
    null::varchar    as status,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
