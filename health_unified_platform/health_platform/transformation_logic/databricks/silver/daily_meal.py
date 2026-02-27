# Databricks notebook source
# Source: health_dw.bronze.stg_lifesum_food (legacy: workspace.default.lifesum_food)
# TODO: Update source reference to health_dw.bronze.stg_lifesum_food when bronze layer is ready

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS health_dw.silver.daily_meal (
# MAGIC   sk_date           INTEGER   NOT NULL,
# MAGIC   date              DATE      NOT NULL,
# MAGIC   meal_type         STRING    NOT NULL,
# MAGIC   food_item         STRING    NOT NULL,
# MAGIC   brand             STRING,
# MAGIC   amount_in_grams   DOUBLE,
# MAGIC   calories          DOUBLE,
# MAGIC   carbs             DOUBLE,
# MAGIC   carbs_fiber       DOUBLE,
# MAGIC   carbs_sugar       DOUBLE,
# MAGIC   cholesterol       DOUBLE,
# MAGIC   fat               DOUBLE,
# MAGIC   fat_saturated     DOUBLE,
# MAGIC   fat_unsaturated   DOUBLE,
# MAGIC   potassium         DOUBLE,
# MAGIC   protein           DOUBLE,
# MAGIC   sodium            DOUBLE,
# MAGIC   business_key_hash STRING    NOT NULL,
# MAGIC   row_hash          STRING    NOT NULL,
# MAGIC   load_datetime     TIMESTAMP NOT NULL,
# MAGIC   update_datetime   TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE health_dw.silver.daily_meal_staging AS
# MAGIC WITH deduped_food AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY date, meal_type, title
# MAGIC             ORDER BY date, meal_type, title
# MAGIC         ) AS rn
# MAGIC     FROM workspace.default.lifesum_food
# MAGIC     WHERE date IS NOT NULL
# MAGIC       AND meal_type IS NOT NULL
# MAGIC       AND title IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     year(date) * 10000 + month(date) * 100 + dayofmonth(date) AS sk_date,
# MAGIC     date,
# MAGIC     meal_type,
# MAGIC     title             AS food_item,
# MAGIC     brand,
# MAGIC     amount_in_grams,
# MAGIC     calories,
# MAGIC     carbs,
# MAGIC     carbs_fiber,
# MAGIC     carbs_sugar,
# MAGIC     cholesterol,
# MAGIC     fat,
# MAGIC     fat_saturated,
# MAGIC     fat_unsaturated,
# MAGIC     potassium,
# MAGIC     protein,
# MAGIC     sodium,
# MAGIC     sha2(concat_ws('||', date, meal_type, title), 256)         AS business_key_hash,
# MAGIC     sha2(
# MAGIC         concat_ws('||',
# MAGIC             amount_in_grams, calories, carbs, carbs_fiber, carbs_sugar,
# MAGIC             cholesterol, fat, fat_saturated, fat_unsaturated,
# MAGIC             potassium, protein, sodium, brand
# MAGIC         ), 256
# MAGIC     ) AS row_hash,
# MAGIC     current_timestamp() AS load_datetime
# MAGIC FROM deduped_food
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO health_dw.silver.daily_meal AS target
# MAGIC USING health_dw.silver.daily_meal_staging AS source
# MAGIC ON target.business_key_hash = source.business_key_hash
# MAGIC
# MAGIC WHEN MATCHED AND target.row_hash <> source.row_hash THEN
# MAGIC   UPDATE SET
# MAGIC     target.sk_date         = source.sk_date,
# MAGIC     target.date            = source.date,
# MAGIC     target.meal_type       = source.meal_type,
# MAGIC     target.food_item       = source.food_item,
# MAGIC     target.brand           = source.brand,
# MAGIC     target.amount_in_grams = source.amount_in_grams,
# MAGIC     target.calories        = source.calories,
# MAGIC     target.carbs           = source.carbs,
# MAGIC     target.carbs_fiber     = source.carbs_fiber,
# MAGIC     target.carbs_sugar     = source.carbs_sugar,
# MAGIC     target.cholesterol     = source.cholesterol,
# MAGIC     target.fat             = source.fat,
# MAGIC     target.fat_saturated   = source.fat_saturated,
# MAGIC     target.fat_unsaturated = source.fat_unsaturated,
# MAGIC     target.potassium       = source.potassium,
# MAGIC     target.protein         = source.protein,
# MAGIC     target.sodium          = source.sodium,
# MAGIC     target.row_hash        = source.row_hash,
# MAGIC     target.update_datetime = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     sk_date, date, meal_type, food_item, brand, amount_in_grams, calories,
# MAGIC     carbs, carbs_fiber, carbs_sugar, cholesterol, fat, fat_saturated,
# MAGIC     fat_unsaturated, potassium, protein, sodium,
# MAGIC     business_key_hash, row_hash, load_datetime, update_datetime
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.sk_date, source.date, source.meal_type, source.food_item, source.brand,
# MAGIC     source.amount_in_grams, source.calories, source.carbs, source.carbs_fiber,
# MAGIC     source.carbs_sugar, source.cholesterol, source.fat, source.fat_saturated,
# MAGIC     source.fat_unsaturated, source.potassium, source.protein, source.sodium,
# MAGIC     source.business_key_hash, source.row_hash, current_timestamp(), current_timestamp()
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_dw.silver.daily_meal_staging;
