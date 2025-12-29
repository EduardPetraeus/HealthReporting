# Databricks notebook source
from pyspark.sql import functions as F

df = (
    spark.range(0, 24)
    .withColumnRenamed("id", "hour")
    .crossJoin(
        spark.range(0, 60).withColumnRenamed("id", "minute")
    )
    .withColumn(
        "sk_time",
        F.concat(
            F.lpad(F.col("hour"), 2, "0"),
            F.lpad(F.col("minute"), 2, "0")
        )
    )
    .withColumn(
        "time_code",
        F.concat(
            F.lpad(F.col("hour"), 2, "0"),
            F.lit(":"),
            F.lpad(F.col("minute"), 2, "0")
        )
    )
    .withColumn("hour_12_code", F.lpad(F.col("hour") % 12, 2, "0"))
    .withColumn("hour_12_key", F.col("hour") % 12)
    .withColumn("minute_code", F.lpad(F.col("minute"), 2, "0"))
    .withColumn("minute_key", F.col("minute"))
    .withColumn("ampm_code", F.when(F.col("hour") < 12, "AM").otherwise("PM"))
    .withColumn("hour_24_code", F.lpad(F.col("hour"), 2, "0"))
    .withColumn("hour_24_key", F.col("hour"))
    .withColumn(
        "minute_15_code",
        F.when(
            F.col("minute") < 15,
            F.concat(
                F.lpad(F.col("hour"), 2, "0"),
                F.lit(":00-"),
                F.lpad(F.col("hour"), 2, "0"),
                F.lit(":15")
            )
        )
        .when(
            (F.col("minute") >= 15) & (F.col("minute") < 30),
            F.concat(
                F.lpad(F.col("hour"), 2, "0"),
                F.lit(":15-"),
                F.lpad(F.col("hour"), 2, "0"),
                F.lit(":30")
            )
        )
        .when(
            (F.col("minute") >= 30) & (F.col("minute") < 45),
            F.concat(
                F.lpad(F.col("hour"), 2, "0"),
                F.lit(":30-"),
                F.lpad(F.col("hour"), 2, "0"),
                F.lit(":45")
            )
        )
        .otherwise(
            F.concat(
                F.lpad(F.col("hour"), 2, "0"),
                F.lit(":45-"),
                F.lpad(((F.col("hour") + 1) % 24), 2, "0"),
                F.lit(":00")
            )
        )
    )
    .withColumn(
        "minute_15_key",
        F.when(F.col("minute") < 15, F.col("hour") * 100)
        .when((F.col("minute") >= 15) & (F.col("minute") < 30), F.col("hour") * 100 + 15)
        .when((F.col("minute") >= 30) & (F.col("minute") < 45), F.col("hour") * 100 + 30)
        .otherwise(F.col("hour") * 100 + 45)
    )
    .select(
        "sk_time",
        "time_code",
        "hour_12_code",
        "hour_12_key",
        "minute_code",
        "minute_key",
        "ampm_code",
        "hour_24_code",
        "hour_24_key",
        "minute_15_code",
        "minute_15_key"
    )
)

df.write.mode("overwrite").saveAsTable("health_dw.silver.time_staging")

# display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO health_dw.silver.time AS target
# MAGIC USING health_dw.silver.time_staging AS source
# MAGIC ON target.sk_time = source.sk_time
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     target.time_code         <> source.time_code OR
# MAGIC     target.hour_12_code      <> source.hour_12_code OR
# MAGIC     target.hour_12_key       <> source.hour_12_key OR
# MAGIC     target.minute_code       <> source.minute_code OR
# MAGIC     target.minute_key        <> source.minute_key OR
# MAGIC     target.ampm_code         <> source.ampm_code OR
# MAGIC     target.hour_24_code      <> source.hour_24_code OR
# MAGIC     target.hour_24_key       <> source.hour_24_key OR
# MAGIC     target.minute_15_code    <> source.minute_15_code OR
# MAGIC     target.minute_15_key     <> source.minute_15_key
# MAGIC )
# MAGIC THEN UPDATE SET
# MAGIC     target.time_code         = source.time_code,
# MAGIC     target.hour_12_code      = source.hour_12_code,
# MAGIC     target.hour_12_key       = source.hour_12_key,
# MAGIC     target.minute_code       = source.minute_code,
# MAGIC     target.minute_key        = source.minute_key,
# MAGIC     target.ampm_code         = source.ampm_code,
# MAGIC     target.hour_24_code      = source.hour_24_code,
# MAGIC     target.hour_24_key       = source.hour_24_key,
# MAGIC     target.minute_15_code    = source.minute_15_code,
# MAGIC     target.minute_15_key     = source.minute_15_key
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     sk_time,
# MAGIC     time_code,
# MAGIC     hour_12_code,
# MAGIC     hour_12_key,
# MAGIC     minute_code,
# MAGIC     minute_key,
# MAGIC     ampm_code,
# MAGIC     hour_24_code,
# MAGIC     hour_24_key,
# MAGIC     minute_15_code,
# MAGIC     minute_15_key
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.sk_time,
# MAGIC     source.time_code,
# MAGIC     source.hour_12_code,
# MAGIC     source.hour_12_key,
# MAGIC     source.minute_code,
# MAGIC     source.minute_key,
# MAGIC     source.ampm_code,
# MAGIC     source.hour_24_code,
# MAGIC     source.hour_24_key,
# MAGIC     source.minute_15_code,
# MAGIC     source.minute_15_key
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_dw.silver.time_staging;
