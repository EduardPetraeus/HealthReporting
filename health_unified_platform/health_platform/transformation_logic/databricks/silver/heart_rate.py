# Databricks notebook source
# Source: health_dw.bronze.stg_oura_heart_rate (legacy: workspace.default.oura_heart_rate)
# TODO: Update source reference to health_dw.bronze.stg_oura_heart_rate when bronze layer is ready

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS health_dw.silver.heart_rate (
# MAGIC   sk_date           INT       NOT NULL,
# MAGIC   sk_time           STRING    NOT NULL,
# MAGIC   timestamp         TIMESTAMP NOT NULL,
# MAGIC   bpm               BIGINT,
# MAGIC   source            STRING,
# MAGIC   business_key_hash STRING    NOT NULL,
# MAGIC   row_hash          STRING    NOT NULL,
# MAGIC   load_datetime     TIMESTAMP NOT NULL,
# MAGIC   update_datetime   TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE health_dw.silver.heart_rate_staging AS
# MAGIC WITH deduped_heart_rate AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY timestamp, source
# MAGIC             ORDER BY timestamp
# MAGIC         ) AS rn
# MAGIC     FROM workspace.default.oura_heart_rate
# MAGIC     WHERE timestamp IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     year(timestamp) * 10000 + month(timestamp) * 100 + dayofmonth(timestamp) AS sk_date,
# MAGIC     lpad(hour(timestamp), 2, '0') || lpad(minute(timestamp), 2, '0')         AS sk_time,
# MAGIC     timestamp,
# MAGIC     bpm,
# MAGIC     source,
# MAGIC     sha2(
# MAGIC         concat_ws('||',
# MAGIC             coalesce(CAST(timestamp AS STRING), ''),
# MAGIC             coalesce(source, '')
# MAGIC         ), 256
# MAGIC     ) AS business_key_hash,
# MAGIC     sha2(
# MAGIC         concat_ws('||',
# MAGIC             CAST(timestamp AS STRING),
# MAGIC             CAST(bpm AS STRING),
# MAGIC             source
# MAGIC         ), 256
# MAGIC     ) AS row_hash,
# MAGIC     current_timestamp() AS load_datetime
# MAGIC FROM deduped_heart_rate
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO health_dw.silver.heart_rate AS target
# MAGIC USING health_dw.silver.heart_rate_staging AS source
# MAGIC ON target.business_key_hash = source.business_key_hash
# MAGIC
# MAGIC WHEN MATCHED AND target.row_hash <> source.row_hash THEN
# MAGIC   UPDATE SET
# MAGIC     target.sk_date           = source.sk_date,
# MAGIC     target.sk_time           = source.sk_time,
# MAGIC     target.timestamp         = source.timestamp,
# MAGIC     target.bpm               = source.bpm,
# MAGIC     target.source            = source.source,
# MAGIC     target.business_key_hash = source.business_key_hash,
# MAGIC     target.row_hash          = source.row_hash,
# MAGIC     target.update_datetime   = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     sk_date, sk_time, timestamp, bpm, source,
# MAGIC     business_key_hash, row_hash, load_datetime, update_datetime
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.sk_date, source.sk_time, source.timestamp, source.bpm, source.source,
# MAGIC     source.business_key_hash, source.row_hash, current_timestamp(), current_timestamp()
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_dw.silver.heart_rate_staging;
