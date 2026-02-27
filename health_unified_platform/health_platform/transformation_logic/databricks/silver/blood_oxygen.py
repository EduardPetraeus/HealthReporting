# Databricks notebook source
# Source: health_dw.bronze.stg_oura_blood_oxygen_level (legacy: workspace.default.oura_blood_oxygen_level)
# TODO: Update source reference to health_dw.bronze.stg_oura_blood_oxygen_level when bronze layer is ready

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS health_dw.silver.blood_oxygen (
# MAGIC   sk_date                     INTEGER   NOT NULL,
# MAGIC   id                          STRING    NOT NULL,
# MAGIC   day                         DATE      NOT NULL,
# MAGIC   breathing_disturbance_index DOUBLE,
# MAGIC   measurement_type            STRING    NOT NULL,
# MAGIC   spo2_percentage             DOUBLE,
# MAGIC   row_hash                    STRING    NOT NULL,
# MAGIC   load_datetime               TIMESTAMP NOT NULL,
# MAGIC   update_datetime             TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE health_dw.silver.blood_oxygen_staging AS
# MAGIC WITH deduped_oxygen AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY id, day
# MAGIC             ORDER BY id, day
# MAGIC         ) AS rn
# MAGIC     FROM workspace.default.oura_blood_oxygen_level
# MAGIC     WHERE id IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     year(day) * 10000 + month(day) * 100 + dayofmonth(day) AS sk_date,
# MAGIC     id,
# MAGIC     day,
# MAGIC     breathing_disturbance_index,
# MAGIC     measurement_type_key AS measurement_type,
# MAGIC     CAST(measurement_value AS DOUBLE) AS spo2_percentage,
# MAGIC     sha2(
# MAGIC         concat_ws(
# MAGIC             '||',
# MAGIC             measurement_type_key,
# MAGIC             CAST(measurement_value AS STRING),
# MAGIC             CAST(breathing_disturbance_index AS STRING)
# MAGIC         ),
# MAGIC         256
# MAGIC     ) AS row_hash,
# MAGIC     current_timestamp() AS load_datetime
# MAGIC FROM deduped_oxygen
# MAGIC LATERAL VIEW explode(
# MAGIC     from_json(spo2_percentage, 'map<string,double>')
# MAGIC ) AS measurement_type_key, measurement_value
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO health_dw.silver.blood_oxygen AS target
# MAGIC USING health_dw.silver.blood_oxygen_staging AS source
# MAGIC ON target.id = source.id AND target.day = source.day
# MAGIC
# MAGIC WHEN MATCHED AND target.row_hash <> source.row_hash THEN
# MAGIC   UPDATE SET
# MAGIC     target.sk_date                      = source.sk_date,
# MAGIC     target.id                           = source.id,
# MAGIC     target.day                          = source.day,
# MAGIC     target.breathing_disturbance_index  = source.breathing_disturbance_index,
# MAGIC     target.measurement_type             = source.measurement_type,
# MAGIC     target.spo2_percentage              = source.spo2_percentage,
# MAGIC     target.row_hash                     = source.row_hash,
# MAGIC     target.update_datetime              = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     sk_date, id, day, breathing_disturbance_index, measurement_type,
# MAGIC     spo2_percentage, row_hash, load_datetime, update_datetime
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.sk_date, source.id, source.day, source.breathing_disturbance_index,
# MAGIC     source.measurement_type, source.spo2_percentage, source.row_hash,
# MAGIC     current_timestamp(), current_timestamp()
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_dw.silver.blood_oxygen_staging;
