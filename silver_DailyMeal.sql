CREATE TABLE workspace.silver.DailyMeal (
  date DATE,
  meal_type STRING,
  title STRING,
  brand STRING,
  serving_name STRING,
  amount DOUBLE,
  amount_in_grams DOUBLE,
  calories DOUBLE,
  carbs DOUBLE,
  carbs_fiber DOUBLE,
  carbs_sugar DOUBLE,
  cholesterol DOUBLE,
  fat DOUBLE,
  fat_saturated DOUBLE,
  fat_unsaturated DOUBLE,
  potassium DOUBLE,
  protein DOUBLE,
  sodium DOUBLE
)
USING DELTA