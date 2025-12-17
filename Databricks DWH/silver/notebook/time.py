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

df.write.mode("overwrite").insertInto("silver.time")

display(df)
