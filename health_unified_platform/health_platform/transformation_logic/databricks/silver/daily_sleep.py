# Databricks notebook source
# Source: health_dw.bronze.stg_oura_daily_sleep (legacy: workspace.default.oura_daily_sleep)
# TODO: Update source reference to health_dw.bronze.stg_oura_daily_sleep when bronze layer is ready

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS health_dw.silver.daily_sleep (
# MAGIC   sk_date         INTEGER   NOT NULL,
# MAGIC   id              STRING    NOT NULL,
# MAGIC   day             DATE      NOT NULL,
# MAGIC   sleep_score     BIGINT,
# MAGIC   timestamp       TIMESTAMP,
# MAGIC   deep_sleep      DOUBLE,
# MAGIC   efficiency      DOUBLE,
# MAGIC   latency         DOUBLE,
# MAGIC   rem_sleep       DOUBLE,
# MAGIC   restfulness     DOUBLE,
# MAGIC   timing          DOUBLE,
# MAGIC   total_sleep     DOUBLE,
# MAGIC   row_hash        STRING    NOT NULL,
# MAGIC   load_datetime   TIMESTAMP NOT NULL,
# MAGIC   update_datetime TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE health_dw.silver.daily_sleep_staging AS
# MAGIC WITH deduped_sleep AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY id, day
# MAGIC             ORDER BY id, day
# MAGIC         ) AS rn
# MAGIC     FROM workspace.default.oura_daily_sleep
# MAGIC     WHERE id IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     year(day) * 10000 + month(day) * 100 + dayofmonth(day) AS sk_date,
# MAGIC     id,
# MAGIC     day,
# MAGIC     score                                                   AS sleep_score,
# MAGIC     timestamp,
# MAGIC     CAST(get_json_object(contributors, '$.deep_sleep')  AS DOUBLE) AS deep_sleep,
# MAGIC     CAST(get_json_object(contributors, '$.efficiency')  AS DOUBLE) AS efficiency,
# MAGIC     CAST(get_json_object(contributors, '$.latency')     AS DOUBLE) AS latency,
# MAGIC     CAST(get_json_object(contributors, '$.rem_sleep')   AS DOUBLE) AS rem_sleep,
# MAGIC     CAST(get_json_object(contributors, '$.restfulness') AS DOUBLE) AS restfulness,
# MAGIC     CAST(get_json_object(contributors, '$.timing')      AS DOUBLE) AS timing,
# MAGIC     CAST(get_json_object(contributors, '$.total_sleep') AS DOUBLE) AS total_sleep,
# MAGIC     sha2(
# MAGIC         concat_ws('||',
# MAGIC             score, timestamp,
# MAGIC             get_json_object(contributors, '$.deep_sleep'),
# MAGIC             get_json_object(contributors, '$.efficiency'),
# MAGIC             get_json_object(contributors, '$.latency'),
# MAGIC             get_json_object(contributors, '$.rem_sleep'),
# MAGIC             get_json_object(contributors, '$.restfulness'),
# MAGIC             get_json_object(contributors, '$.timing'),
# MAGIC             get_json_object(contributors, '$.total_sleep')
# MAGIC         ), 256
# MAGIC     ) AS row_hash,
# MAGIC     current_timestamp() AS load_datetime
# MAGIC FROM deduped_sleep
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO health_dw.silver.daily_sleep AS target
# MAGIC USING health_dw.silver.daily_sleep_staging AS source
# MAGIC ON target.id = source.id AND target.day = source.day
# MAGIC
# MAGIC WHEN MATCHED AND target.row_hash <> source.row_hash THEN
# MAGIC   UPDATE SET
# MAGIC     target.sk_date         = source.sk_date,
# MAGIC     target.sleep_score     = source.sleep_score,
# MAGIC     target.timestamp       = source.timestamp,
# MAGIC     target.deep_sleep      = source.deep_sleep,
# MAGIC     target.efficiency      = source.efficiency,
# MAGIC     target.latency         = source.latency,
# MAGIC     target.rem_sleep       = source.rem_sleep,
# MAGIC     target.restfulness     = source.restfulness,
# MAGIC     target.timing          = source.timing,
# MAGIC     target.total_sleep     = source.total_sleep,
# MAGIC     target.row_hash        = source.row_hash,
# MAGIC     target.update_datetime = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     sk_date, id, day, sleep_score, timestamp, deep_sleep, efficiency,
# MAGIC     latency, rem_sleep, restfulness, timing, total_sleep,
# MAGIC     row_hash, load_datetime, update_datetime
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.sk_date, source.id, source.day, source.sleep_score, source.timestamp,
# MAGIC     source.deep_sleep, source.efficiency, source.latency, source.rem_sleep,
# MAGIC     source.restfulness, source.timing, source.total_sleep,
# MAGIC     source.row_hash, current_timestamp(), current_timestamp()
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_dw.silver.daily_sleep_staging;
