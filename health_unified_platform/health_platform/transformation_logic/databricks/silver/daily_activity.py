# Databricks notebook source
# Source: health_dw.bronze.stg_oura_daily_activity (legacy: workspace.default.oura_daily_activity)
# TODO: Update source reference to health_dw.bronze.stg_oura_daily_activity when bronze layer is ready

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS health_dw.silver.daily_activity (
# MAGIC   sk_date                     INTEGER   NOT NULL,
# MAGIC   id                          STRING    NOT NULL,
# MAGIC   day                         DATE      NOT NULL,
# MAGIC   activity_score              BIGINT,
# MAGIC   steps                       BIGINT,
# MAGIC   equivalent_walking_distance BIGINT,
# MAGIC   inactivity_alerts           BIGINT,
# MAGIC   target_calories             BIGINT,
# MAGIC   active_calories             BIGINT,
# MAGIC   meet_daily_targets          DOUBLE,
# MAGIC   move_every_hour             DOUBLE,
# MAGIC   recovery_time               DOUBLE,
# MAGIC   stay_active                 DOUBLE,
# MAGIC   training_frequency          DOUBLE,
# MAGIC   training_volume             DOUBLE,
# MAGIC   row_hash                    STRING    NOT NULL,
# MAGIC   load_datetime               TIMESTAMP NOT NULL,
# MAGIC   update_datetime             TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE health_dw.silver.daily_activity_staging AS
# MAGIC WITH deduped_activity AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY id, day
# MAGIC             ORDER BY id, day
# MAGIC         ) AS rn
# MAGIC     FROM workspace.default.oura_daily_activity
# MAGIC     WHERE id IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     year(day) * 10000 + month(day) * 100 + dayofmonth(day) AS sk_date,
# MAGIC     id,
# MAGIC     day,
# MAGIC     score                                                   AS activity_score,
# MAGIC     steps,
# MAGIC     equivalent_walking_distance,
# MAGIC     inactivity_alerts,
# MAGIC     target_calories,
# MAGIC     active_calories,
# MAGIC     CAST(get_json_object(contributors, '$.meet_daily_targets') AS DOUBLE) AS meet_daily_targets,
# MAGIC     CAST(get_json_object(contributors, '$.move_every_hour')    AS DOUBLE) AS move_every_hour,
# MAGIC     CAST(get_json_object(contributors, '$.recovery_time')      AS DOUBLE) AS recovery_time,
# MAGIC     CAST(get_json_object(contributors, '$.stay_active')        AS DOUBLE) AS stay_active,
# MAGIC     CAST(get_json_object(contributors, '$.training_frequency') AS DOUBLE) AS training_frequency,
# MAGIC     CAST(get_json_object(contributors, '$.training_volume')    AS DOUBLE) AS training_volume,
# MAGIC     sha2(
# MAGIC         concat_ws('||',
# MAGIC             score, steps, equivalent_walking_distance, inactivity_alerts,
# MAGIC             target_calories, active_calories,
# MAGIC             get_json_object(contributors, '$.meet_daily_targets'),
# MAGIC             get_json_object(contributors, '$.move_every_hour'),
# MAGIC             get_json_object(contributors, '$.recovery_time'),
# MAGIC             get_json_object(contributors, '$.stay_active'),
# MAGIC             get_json_object(contributors, '$.training_frequency'),
# MAGIC             get_json_object(contributors, '$.training_volume')
# MAGIC         ), 256
# MAGIC     ) AS row_hash,
# MAGIC     current_timestamp() AS load_datetime
# MAGIC FROM deduped_activity
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO health_dw.silver.daily_activity AS target
# MAGIC USING health_dw.silver.daily_activity_staging AS source
# MAGIC ON target.id = source.id AND target.day = source.day
# MAGIC
# MAGIC WHEN MATCHED AND target.row_hash <> source.row_hash THEN
# MAGIC   UPDATE SET
# MAGIC     target.sk_date                      = source.sk_date,
# MAGIC     target.activity_score               = source.activity_score,
# MAGIC     target.steps                        = source.steps,
# MAGIC     target.equivalent_walking_distance  = source.equivalent_walking_distance,
# MAGIC     target.inactivity_alerts            = source.inactivity_alerts,
# MAGIC     target.target_calories              = source.target_calories,
# MAGIC     target.active_calories              = source.active_calories,
# MAGIC     target.meet_daily_targets           = source.meet_daily_targets,
# MAGIC     target.move_every_hour              = source.move_every_hour,
# MAGIC     target.recovery_time                = source.recovery_time,
# MAGIC     target.stay_active                  = source.stay_active,
# MAGIC     target.training_frequency           = source.training_frequency,
# MAGIC     target.training_volume              = source.training_volume,
# MAGIC     target.row_hash                     = source.row_hash,
# MAGIC     target.update_datetime              = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     sk_date, id, day, activity_score, steps, equivalent_walking_distance,
# MAGIC     inactivity_alerts, target_calories, active_calories, meet_daily_targets,
# MAGIC     move_every_hour, recovery_time, stay_active, training_frequency,
# MAGIC     training_volume, row_hash, load_datetime, update_datetime
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.sk_date, source.id, source.day, source.activity_score, source.steps,
# MAGIC     source.equivalent_walking_distance, source.inactivity_alerts, source.target_calories,
# MAGIC     source.active_calories, source.meet_daily_targets, source.move_every_hour,
# MAGIC     source.recovery_time, source.stay_active, source.training_frequency,
# MAGIC     source.training_volume, source.row_hash, current_timestamp(), current_timestamp()
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_dw.silver.daily_activity_staging;
