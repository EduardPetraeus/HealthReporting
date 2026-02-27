{{
   config(materialized='table')
}}

-- Schema-only definition for silver.daily_annotations.
-- Run once with: dbt run --select daily_annotations
-- No merge script -- rows inserted directly as needed (manually curated, insert-only).

select
    null::integer    as sk_date,
    null::varchar    as annotation_type,
    null::varchar    as annotation,
    null::varchar    as created_by,
    null::boolean    as is_valid
where false
