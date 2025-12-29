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

display(df)

df.write.format("delta").mode("overwrite").saveAsTable("health_dw.silver.date")
