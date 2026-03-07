{{
   config(materialized='table')
}}

-- Schema-only definition for silver.supplement_log.
-- Run once with: dbt run --select supplement_log
-- Data is loaded by: ingestion/import_manual_data.py

select
    null::varchar    as supplement_name,
    null::double     as dose,
    null::varchar    as unit,
    null::varchar    as frequency,
    null::varchar    as timing,
    null::varchar    as product,
    null::date       as start_date,
    null::date       as end_date,
    null::varchar    as status,
    null::varchar    as target,
    null::varchar    as notes,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
