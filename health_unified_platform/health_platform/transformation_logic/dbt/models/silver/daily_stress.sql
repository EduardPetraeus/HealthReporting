{{
    config(materialized='table')
}}

-- Schema-only definition for silver.daily_stress.
-- Run once with: dbt run --select daily_stress
-- Data is loaded by: dbt/merge/silver/merge_oura_daily_stress.sql

select
    null::integer    as sk_date,
    null::date       as day,
    null::varchar    as day_summary,
    null::integer    as stress_high,
    null::integer    as recovery_high,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
