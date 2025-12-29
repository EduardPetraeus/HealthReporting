# Databricks notebook source
from pyspark.sql.functions import (
    year, quarter, month, weekofyear, dayofweek, dayofyear, dayofmonth,
    date_format, concat, lpad, when, lower
)
from datetime import datetime, timedelta

start_date = datetime(2017, 1, 1)
end_date = datetime.today() + timedelta(days=2*365)

dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
df = spark.createDataFrame([(d,) for d in dates], ["date"])

df = (
    df.withColumn("sk_date", year("date") * 10000 + month("date") * 100 + dayofmonth("date"))
      .withColumn("year", year("date"))
      .withColumn("quarter", quarter("date"))
      .withColumn("month", month("date"))
      .withColumn("week", weekofyear("date"))
      .withColumn("day_of_week", dayofweek("date"))
      .withColumn("day_of_year", dayofyear("date"))
      .withColumn("day_of_month", dayofmonth("date"))
      .withColumn("month_name", lower(date_format("date", "MMMM")))
      .withColumn("day_name", lower(date_format("date", "EEEE")))
      .withColumn("is_leap_year", when(
          (year("date") % 4 == 0) & ((year("date") % 100 != 0) | (year("date") % 400 == 0)), 1
      ).otherwise(0))
      .withColumn("year_month", concat(year("date"), lpad(month("date"), 2, "0")))
      .withColumn("is_weekend", when(
          (dayofweek("date") == 1) | (dayofweek("date") == 7), 1
      ).otherwise(0))
)

# display(df)

df.write.format("delta").mode("overwrite").saveAsTable("health_dw.silver.date_staging")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO health_dw.silver.date AS target
# MAGIC USING health_dw.silver.date_staging AS source
# MAGIC ON target.sk_date = source.sk_date
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.year <> source.year OR
# MAGIC     target.quarter <> source.quarter OR
# MAGIC     target.month <> source.month OR
# MAGIC     target.week <> source.week OR
# MAGIC     target.day_of_week <> source.day_of_week OR
# MAGIC     target.day_of_year <> source.day_of_year OR
# MAGIC     target.day_of_month <> source.day_of_month OR
# MAGIC     target.month_name <> source.month_name OR
# MAGIC     target.day_name <> source.day_name OR
# MAGIC     target.is_leap_year <> source.is_leap_year OR
# MAGIC     target.year_month <> source.year_month OR
# MAGIC     target.is_weekend <> source.is_weekend
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.date = source.date,
# MAGIC     target.year = source.year,
# MAGIC     target.quarter = source.quarter,
# MAGIC     target.month = source.month,
# MAGIC     target.week = source.week,
# MAGIC     target.day_of_week = source.day_of_week,
# MAGIC     target.day_of_year = source.day_of_year,
# MAGIC     target.day_of_month = source.day_of_month,
# MAGIC     target.month_name = source.month_name,
# MAGIC     target.day_name = source.day_name,
# MAGIC     target.is_leap_year = source.is_leap_year,
# MAGIC     target.year_month = source.year_month,
# MAGIC     target.is_weekend = source.is_weekend
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     date, sk_date, year, quarter, month, week, day_of_week, day_of_year, day_of_month,
# MAGIC     month_name, day_name, is_leap_year, year_month, is_weekend
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.date, source.sk_date, source.year, source.quarter, source.month, source.week,
# MAGIC     source.day_of_week, source.day_of_year, source.day_of_month, source.month_name,
# MAGIC     source.day_name, source.is_leap_year, source.year_month, source.is_weekend
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_dw.silver.date_staging;
