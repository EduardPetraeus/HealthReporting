%sql
CREATE TABLE workspace.silver.date (
  date DATE,
  sk_date INT,
  year INT,
  quarter INT,
  month INT,
  week INT,
  day_of_week INT,
  day_of_year INT,
  day_of_month INT,
  month_name STRING,
  day_name STRING,
  is_leap_year INT,
  year_month STRING,
  is_weekend INT
)
USING DELTA;