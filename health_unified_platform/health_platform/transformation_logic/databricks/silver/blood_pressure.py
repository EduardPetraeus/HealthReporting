# Databricks notebook source
# Source: health_dw.bronze.stg_withings_blood_pressure (legacy: workspace.default.withings_blood_pressure)
# TODO: Update source reference to health_dw.bronze.stg_withings_blood_pressure when bronze layer is ready

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS health_dw.silver.blood_pressure (
# MAGIC   sk_date           INTEGER   NOT NULL,
# MAGIC   sk_time           STRING    NOT NULL,
# MAGIC   datetime          TIMESTAMP NOT NULL,
# MAGIC   systolic          BIGINT    NOT NULL,
# MAGIC   diastolic         BIGINT    NOT NULL,
# MAGIC   comments          STRING,
# MAGIC   business_key_hash STRING    NOT NULL,
# MAGIC   row_hash          STRING    NOT NULL,
# MAGIC   load_datetime     TIMESTAMP NOT NULL,
# MAGIC   update_datetime   TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE health_dw.silver.blood_pressure_staging AS
# MAGIC WITH deduped_bp AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY Date, Systolic, Diastolic
# MAGIC             ORDER BY Date
# MAGIC         ) AS rn
# MAGIC     FROM workspace.default.withings_blood_pressure
# MAGIC     WHERE Date IS NOT NULL
# MAGIC       AND Systolic IS NOT NULL
# MAGIC       AND Diastolic IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     year(Date) * 10000 + month(Date) * 100 + dayofmonth(Date)  AS sk_date,
# MAGIC     lpad(hour(Date), 2, '0') || lpad(minute(Date), 2, '0')     AS sk_time,
# MAGIC     Date                                                         AS datetime,
# MAGIC     Systolic                                                     AS systolic,
# MAGIC     Diastolic                                                    AS diastolic,
# MAGIC     Comments                                                     AS comments,
# MAGIC     sha2(
# MAGIC         concat_ws('||',
# MAGIC             coalesce(cast(Date AS STRING), ''),
# MAGIC             coalesce(cast(Systolic AS STRING), ''),
# MAGIC             coalesce(cast(Diastolic AS STRING), '')
# MAGIC         ), 256
# MAGIC     ) AS business_key_hash,
# MAGIC     sha2(
# MAGIC         concat_ws('||',
# MAGIC             coalesce(cast(Systolic AS STRING), ''),
# MAGIC             coalesce(cast(Diastolic AS STRING), ''),
# MAGIC             coalesce(Comments, '')
# MAGIC         ), 256
# MAGIC     ) AS row_hash,
# MAGIC     current_timestamp() AS load_datetime
# MAGIC FROM deduped_bp
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO health_dw.silver.blood_pressure AS target
# MAGIC USING health_dw.silver.blood_pressure_staging AS source
# MAGIC ON target.business_key_hash = source.business_key_hash
# MAGIC
# MAGIC WHEN MATCHED AND target.row_hash <> source.row_hash THEN
# MAGIC   UPDATE SET
# MAGIC     target.sk_date           = source.sk_date,
# MAGIC     target.sk_time           = source.sk_time,
# MAGIC     target.datetime          = source.datetime,
# MAGIC     target.systolic          = source.systolic,
# MAGIC     target.diastolic         = source.diastolic,
# MAGIC     target.comments          = source.comments,
# MAGIC     target.business_key_hash = source.business_key_hash,
# MAGIC     target.row_hash          = source.row_hash,
# MAGIC     target.update_datetime   = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     sk_date, sk_time, datetime, systolic, diastolic, comments,
# MAGIC     business_key_hash, row_hash, load_datetime, update_datetime
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.sk_date, source.sk_time, source.datetime, source.systolic,
# MAGIC     source.diastolic, source.comments, source.business_key_hash, source.row_hash,
# MAGIC     current_timestamp(), current_timestamp()
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_dw.silver.blood_pressure_staging;
