-- merge_lifesum_food.sql
-- Per-source merge: Lifesum food export -> silver.daily_meal
-- Full load pattern: source always contains complete history.
--
-- Usage: python run_merge.py silver/merge_lifesum_food.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.daily_meal__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date, meal_type, title
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_lifesum_food
    WHERE date IS NOT NULL
      AND meal_type IS NOT NULL
      AND title IS NOT NULL
)
SELECT
    -- Surrogate date key
    (year(date::DATE) * 10000 + month(date::DATE) * 100 + day(date::DATE))::INTEGER AS sk_date,

    -- Business columns
    date::DATE          AS date,
    meal_type,
    title               AS food_item,
    brand,
    serving_name,
    amount,
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

    -- Deterministic business key hash (identity: one row per date + meal + food item)
    md5(
        coalesce(date, '') || '||' ||
        coalesce(meal_type, '') || '||' ||
        coalesce(title, '')
    ) AS business_key_hash,

    -- Row hash (change detection on all measurable values)
    md5(
        coalesce(cast(amount AS VARCHAR), '') || '||' ||
        coalesce(cast(amount_in_grams AS VARCHAR), '') || '||' ||
        coalesce(serving_name, '') || '||' ||
        coalesce(cast(calories AS VARCHAR), '') || '||' ||
        coalesce(cast(carbs AS VARCHAR), '') || '||' ||
        coalesce(cast(carbs_fiber AS VARCHAR), '') || '||' ||
        coalesce(cast(carbs_sugar AS VARCHAR), '') || '||' ||
        coalesce(cast(cholesterol AS VARCHAR), '') || '||' ||
        coalesce(cast(fat AS VARCHAR), '') || '||' ||
        coalesce(cast(fat_saturated AS VARCHAR), '') || '||' ||
        coalesce(cast(fat_unsaturated AS VARCHAR), '') || '||' ||
        coalesce(cast(potassium AS VARCHAR), '') || '||' ||
        coalesce(cast(protein AS VARCHAR), '') || '||' ||
        coalesce(cast(sodium AS VARCHAR), '') || '||' ||
        coalesce(brand, '')
    ) AS row_hash,

    current_timestamp AS load_datetime

FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.daily_meal
MERGE INTO silver.daily_meal AS target
USING silver.daily_meal__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    date              = src.date,
    meal_type         = src.meal_type,
    food_item         = src.food_item,
    brand             = src.brand,
    serving_name      = src.serving_name,
    amount            = src.amount,
    amount_in_grams   = src.amount_in_grams,
    calories          = src.calories,
    carbs             = src.carbs,
    carbs_fiber       = src.carbs_fiber,
    carbs_sugar       = src.carbs_sugar,
    cholesterol       = src.cholesterol,
    fat               = src.fat,
    fat_saturated     = src.fat_saturated,
    fat_unsaturated   = src.fat_unsaturated,
    potassium         = src.potassium,
    protein           = src.protein,
    sodium            = src.sodium,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date,
    date,
    meal_type,
    food_item,
    brand,
    serving_name,
    amount,
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
    src.sk_date,
    src.date,
    src.meal_type,
    src.food_item,
    src.brand,
    src.serving_name,
    src.amount,
    src.amount_in_grams,
    src.calories,
    src.carbs,
    src.carbs_fiber,
    src.carbs_sugar,
    src.cholesterol,
    src.fat,
    src.fat_saturated,
    src.fat_unsaturated,
    src.potassium,
    src.protein,
    src.sodium,
    src.business_key_hash,
    src.row_hash,
    current_timestamp,
    current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.daily_meal__staging;
