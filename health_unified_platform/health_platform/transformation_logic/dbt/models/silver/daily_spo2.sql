{{
    config(materialized='table')
}}

-- Schema-only definition for silver.daily_spo2.
-- Run once with: dbt run --select daily_spo2
-- Data is loaded by: dbt/merge/silver/merge_oura_daily_spo2.sql

select
    null::integer    as sk_date,
    null::date       as day,
    null::double     as spo2_avg_pct,
    null::double     as breathing_disturbance_index,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
