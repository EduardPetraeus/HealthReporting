{{
    config(
        materialized='table'
    )
}}

-- Schema-only definition for silver.daily_meal.
-- Run once with: dbt run --select daily_meal
-- Data is loaded by per-source merge scripts in dbt/merge/silver/.

select
    null::integer    as sk_date,
    null::date       as date,
    null::varchar    as meal_type,
    null::varchar    as food_item,
    null::varchar    as brand,
    null::varchar    as serving_name,
    null::double     as amount,
    null::double     as amount_in_grams,
    null::double     as calories,
    null::double     as carbs,
    null::double     as carbs_fiber,
    null::double     as carbs_sugar,
    null::double     as cholesterol,
    null::double     as fat,
    null::double     as fat_saturated,
    null::double     as fat_unsaturated,
    null::double     as potassium,
    null::double     as protein,
    null::double     as sodium,
    null::varchar    as business_key_hash,
    null::varchar    as row_hash,
    null::timestamp  as load_datetime,
    null::timestamp  as update_datetime
where false
