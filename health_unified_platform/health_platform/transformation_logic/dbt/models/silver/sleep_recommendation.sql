{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.sleep_recommendation.
-- Run once with: dbt run --select sleep_recommendation
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as date,
    null::varchar    as recommendation,
    null::varchar    as status,
    null::integer    as optimal_start_offset,
    null::integer    as optimal_end_offset,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
