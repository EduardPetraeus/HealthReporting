CREATE TABLE health_dw.silver.daily_meal (
  sk_date INTEGER NOT NULL,
  date DATE NOT NULL,
  meal_type STRING NOT NULL,
  food_item STRING NOT NULL,
  brand STRING,
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
  sodium DOUBLE,
  business_key_hash STRING NOT NULL,
  row_hash STRING NOT NULL,
  load_datetime TIMESTAMP NOT NULL,
  update_datetime TIMESTAMP NOT NULL
)
USING DELTA