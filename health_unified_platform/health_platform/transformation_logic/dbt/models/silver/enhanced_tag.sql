{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.enhanced_tag.
-- Run once with: dbt run --select enhanced_tag
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as start_day,
    null::varchar    as start_time,
    null::date       as end_day,
    null::varchar    as end_time,
    null::varchar    as tag_type_code,
    null::varchar    as custom_tag_name,
    null::varchar    as comment,
    null::varchar    as source_name,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
