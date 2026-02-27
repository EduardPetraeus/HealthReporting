# Databricks notebook source
# Source: health_dw.bronze.stg_oura_daily_readiness (legacy: workspace.default.oura_daily_readiness)
# TODO: Update source reference to health_dw.bronze.stg_oura_daily_readiness when bronze layer is ready

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS health_dw.silver.daily_readiness (
# MAGIC   sk_date                     INTEGER   NOT NULL,
# MAGIC   id                          STRING    NOT NULL,
# MAGIC   day                         DATE      NOT NULL,
# MAGIC   readiness_score             BIGINT,
# MAGIC   temperature_deviation       DOUBLE,
# MAGIC   temperature_trend_deviation DOUBLE,
# MAGIC   timestamp                   TIMESTAMP,
# MAGIC   activity_balance            DOUBLE,
# MAGIC   body_temperature            DOUBLE,
# MAGIC   hrv_balance                 DOUBLE,
# MAGIC   previous_day_activity       DOUBLE,
# MAGIC   previous_night              DOUBLE,
# MAGIC   recovery_index              DOUBLE,
# MAGIC   resting_heart_rate          DOUBLE,
# MAGIC   sleep_balance               DOUBLE,
# MAGIC   sleep_regularity            DOUBLE,
# MAGIC   row_hash                    STRING    NOT NULL,
# MAGIC   load_datetime               TIMESTAMP NOT NULL,
# MAGIC   update_datetime             TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE health_dw.silver.daily_readiness_staging AS
# MAGIC WITH deduped_readiness AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY id
# MAGIC             ORDER BY id
# MAGIC         ) AS rn
# MAGIC     FROM workspace.default.oura_daily_readiness
# MAGIC     WHERE id IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     year(day) * 10000 + month(day) * 100 + dayofmonth(day) AS sk_date,
# MAGIC     id,
# MAGIC     day,
# MAGIC     score                                                   AS readiness_score,
# MAGIC     temperature_deviation,
# MAGIC     temperature_trend_deviation,
# MAGIC     timestamp,
# MAGIC     CAST(get_json_object(contributors, '$.activity_balance')     AS DOUBLE) AS activity_balance,
# MAGIC     CAST(get_json_object(contributors, '$.body_temperature')     AS DOUBLE) AS body_temperature,
# MAGIC     CAST(get_json_object(contributors, '$.hrv_balance')          AS DOUBLE) AS hrv_balance,
# MAGIC     CAST(get_json_object(contributors, '$.previous_day_activity') AS DOUBLE) AS previous_day_activity,
# MAGIC     CAST(get_json_object(contributors, '$.previous_night')       AS DOUBLE) AS previous_night,
# MAGIC     CAST(get_json_object(contributors, '$.recovery_index')       AS DOUBLE) AS recovery_index,
# MAGIC     CAST(get_json_object(contributors, '$.resting_heart_rate')   AS DOUBLE) AS resting_heart_rate,
# MAGIC     CAST(get_json_object(contributors, '$.sleep_balance')        AS DOUBLE) AS sleep_balance,
# MAGIC     CAST(get_json_object(contributors, '$.sleep_regularity')     AS DOUBLE) AS sleep_regularity,
# MAGIC     sha2(
# MAGIC         concat_ws('||',
# MAGIC             score, temperature_deviation, temperature_trend_deviation,
# MAGIC             get_json_object(contributors, '$.activity_balance'),
# MAGIC             get_json_object(contributors, '$.body_temperature'),
# MAGIC             get_json_object(contributors, '$.hrv_balance'),
# MAGIC             get_json_object(contributors, '$.previous_day_activity'),
# MAGIC             get_json_object(contributors, '$.previous_night'),
# MAGIC             get_json_object(contributors, '$.recovery_index'),
# MAGIC             get_json_object(contributors, '$.resting_heart_rate'),
# MAGIC             get_json_object(contributors, '$.sleep_balance'),
# MAGIC             get_json_object(contributors, '$.sleep_regularity')
# MAGIC         ), 256
# MAGIC     ) AS row_hash,
# MAGIC     current_timestamp() AS load_datetime
# MAGIC FROM deduped_readiness
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO health_dw.silver.daily_readiness AS target
# MAGIC USING health_dw.silver.daily_readiness_staging AS source
# MAGIC ON target.id = source.id AND target.day = source.day
# MAGIC
# MAGIC WHEN MATCHED AND target.row_hash <> source.row_hash THEN
# MAGIC   UPDATE SET
# MAGIC     target.sk_date                      = source.sk_date,
# MAGIC     target.id                           = source.id,
# MAGIC     target.day                          = source.day,
# MAGIC     target.readiness_score              = source.readiness_score,
# MAGIC     target.temperature_deviation        = source.temperature_deviation,
# MAGIC     target.temperature_trend_deviation  = source.temperature_trend_deviation,
# MAGIC     target.timestamp                    = source.timestamp,
# MAGIC     target.activity_balance             = source.activity_balance,
# MAGIC     target.body_temperature             = source.body_temperature,
# MAGIC     target.hrv_balance                  = source.hrv_balance,
# MAGIC     target.previous_day_activity        = source.previous_day_activity,
# MAGIC     target.previous_night               = source.previous_night,
# MAGIC     target.recovery_index               = source.recovery_index,
# MAGIC     target.resting_heart_rate           = source.resting_heart_rate,
# MAGIC     target.sleep_balance                = source.sleep_balance,
# MAGIC     target.sleep_regularity             = source.sleep_regularity,
# MAGIC     target.row_hash                     = source.row_hash,
# MAGIC     target.update_datetime              = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     sk_date, id, day, readiness_score, temperature_deviation, temperature_trend_deviation,
# MAGIC     timestamp, activity_balance, body_temperature, hrv_balance, previous_day_activity,
# MAGIC     previous_night, recovery_index, resting_heart_rate, sleep_balance, sleep_regularity,
# MAGIC     row_hash, load_datetime, update_datetime
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.sk_date, source.id, source.day, source.readiness_score,
# MAGIC     source.temperature_deviation, source.temperature_trend_deviation, source.timestamp,
# MAGIC     source.activity_balance, source.body_temperature, source.hrv_balance,
# MAGIC     source.previous_day_activity, source.previous_night, source.recovery_index,
# MAGIC     source.resting_heart_rate, source.sleep_balance, source.sleep_regularity,
# MAGIC     source.row_hash, current_timestamp(), current_timestamp()
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_dw.silver.daily_readiness_staging;
