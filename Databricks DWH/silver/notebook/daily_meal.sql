-- Databricks notebook source
-- MAGIC %md
-- MAGIC Build a core data model for the food consumed

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.silver.daily_meal_staging AS
WITH deduped_food AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date, meal_type, title
            ORDER BY date, meal_type, title
        ) AS rn
    FROM workspace.default.food
    WHERE date IS NOT NULL
      AND meal_type IS NOT NULL
      AND title IS NOT NULL
)
SELECT
    -- Surrogate date key
    year(date) * 10000 + month(date) * 100 + dayofmonth(date) AS sk_date,

    -- Business columns
    date,
    meal_type,
    title AS food_item,
    brand,
    amount_in_grams,
    calories,
    carbs,
    carbs_fiber,
    carbs_sugar,
    cholesterol,
    fat,
    fat_saturated,
    fat_unsaturated,
    potassium,
    protein,
    sodium,

    -- Deterministic business key hash (identity)
    sha2(
        concat_ws('||', date, meal_type, title),
        256
    ) AS business_key_hash,

    -- Row hash (change detection)
    sha2(
        concat_ws(
            '||',
            amount_in_grams,
            calories,
            carbs,
            carbs_fiber,
            carbs_sugar,
            cholesterol,
            fat,
            fat_saturated,
            fat_unsaturated,
            potassium,
            protein,
            sodium,
            brand
        ),
        256
    ) AS row_hash,

    current_timestamp() AS load_datetime
FROM deduped_food
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO workspace.silver.daily_meal AS target
USING workspace.silver.daily_meal_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED
  AND target.row_hash <> source.row_hash
THEN UPDATE SET
    target.sk_date           = source.sk_date,
    target.date              = source.date,
    target.meal_type         = source.meal_type,
    target.food_item         = source.food_item,
    target.brand             = source.brand,
    target.amount_in_grams   = source.amount_in_grams,
    target.calories          = source.calories,
    target.carbs             = source.carbs,
    target.carbs_fiber       = source.carbs_fiber,
    target.carbs_sugar       = source.carbs_sugar,
    target.cholesterol       = source.cholesterol,
    target.fat               = source.fat,
    target.fat_saturated     = source.fat_saturated,
    target.fat_unsaturated   = source.fat_unsaturated,
    target.potassium         = source.potassium,
    target.protein           = source.protein,
    target.sodium            = source.sodium,
    target.row_hash          = source.row_hash,
    target.update_datetime   = current_timestamp()

WHEN NOT MATCHED THEN
  INSERT (
      sk_date,
      date,
      meal_type,
      food_item,
      brand,
      amount_in_grams,
      calories,
      carbs,
      carbs_fiber,
      carbs_sugar,
      cholesterol,
      fat,
      fat_saturated,
      fat_unsaturated,
      potassium,
      protein,
      sodium,
      business_key_hash,
      row_hash,
      load_datetime,
      update_datetime
  )
  VALUES (
      source.sk_date,
      source.date,
      source.meal_type,
      source.food_item,
      source.brand,
      source.amount_in_grams,
      source.calories,
      source.carbs,
      source.carbs_fiber,
      source.carbs_sugar,
      source.cholesterol,
      source.fat,
      source.fat_saturated,
      source.fat_unsaturated,
      source.potassium,
      source.protein,
      source.sodium,
      source.business_key_hash,
      source.row_hash,
      current_timestamp(),
      current_timestamp()
  );

-- COMMAND ----------

DROP TABLE IF EXISTS workspace.silver.daily_meal_staging;
