{{
    config(materialized='table')
}}

-- Schema-only definition for silver.daily_sleep.
-- Run once with: dbt run --select daily_sleep
-- Data is loaded by: dbt/merge/silver/merge_oura_daily_sleep.sql

select
    null::integer    as sk_date,
    null::date       as day,
    null::integer    as sleep_score,
    null::timestamp  as timestamp,
    null::integer    as contributor_deep_sleep,
    null::integer    as contributor_efficiency,
    null::integer    as contributor_latency,
    null::integer    as contributor_rem_sleep,
    null::integer    as contributor_restfulness,
    null::integer    as contributor_timing,
    null::integer    as contributor_total_sleep,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
