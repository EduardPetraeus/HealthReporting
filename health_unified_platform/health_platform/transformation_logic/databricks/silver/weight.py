# Databricks notebook source
# Source: health_dw.bronze.stg_withings_weight (legacy: workspace.default.withings_weight)
# TODO: Update source reference to health_dw.bronze.stg_withings_weight when bronze layer is ready

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS health_dw.silver.weight (
# MAGIC   sk_date           INTEGER   NOT NULL,
# MAGIC   sk_time           STRING    NOT NULL,
# MAGIC   datetime          TIMESTAMP NOT NULL,
# MAGIC   weight_kg         DOUBLE    NOT NULL,
# MAGIC   fat_mass_kg       DOUBLE,
# MAGIC   bone_mass_kg      DOUBLE,
# MAGIC   muscle_mass_kg    DOUBLE,
# MAGIC   hydration_kg      DOUBLE,
# MAGIC   business_key_hash STRING    NOT NULL,
# MAGIC   row_hash          STRING    NOT NULL,
# MAGIC   load_datetime     TIMESTAMP NOT NULL,
# MAGIC   update_datetime   TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE health_dw.silver.weight_staging AS
# MAGIC WITH deduped_weight AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY Date, `Weight (kg)`, `Fat mass (kg)`, `Bone mass (kg)`, `Muscle mass (kg)`, `Hydration (kg)`
# MAGIC             ORDER BY Date
# MAGIC         ) AS rn
# MAGIC     FROM workspace.default.withings_weight
# MAGIC     WHERE Date IS NOT NULL
# MAGIC       AND `Weight (kg)` IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC     year(Date) * 10000 + month(Date) * 100 + dayofmonth(Date) AS sk_date,
# MAGIC     lpad(hour(Date), 2, '0') || lpad(minute(Date), 2, '0')    AS sk_time,
# MAGIC     Date                                                        AS datetime,
# MAGIC     `Weight (kg)`                                               AS weight_kg,
# MAGIC     `Fat mass (kg)`                                             AS fat_mass_kg,
# MAGIC     `Bone mass (kg)`                                            AS bone_mass_kg,
# MAGIC     `Muscle mass (kg)`                                          AS muscle_mass_kg,
# MAGIC     `Hydration (kg)`                                            AS hydration_kg,
# MAGIC     sha2(
# MAGIC         concat_ws('||',
# MAGIC             coalesce(cast(Date AS STRING), ''),
# MAGIC             coalesce(cast(`Weight (kg)` AS STRING), ''),
# MAGIC             coalesce(cast(`Fat mass (kg)` AS STRING), ''),
# MAGIC             coalesce(cast(`Bone mass (kg)` AS STRING), ''),
# MAGIC             coalesce(cast(`Muscle mass (kg)` AS STRING), ''),
# MAGIC             coalesce(cast(`Hydration (kg)` AS STRING), '')
# MAGIC         ), 256
# MAGIC     ) AS business_key_hash,
# MAGIC     sha2(
# MAGIC         concat_ws('||',
# MAGIC             coalesce(cast(`Weight (kg)` AS STRING), ''),
# MAGIC             coalesce(cast(`Fat mass (kg)` AS STRING), ''),
# MAGIC             coalesce(cast(`Bone mass (kg)` AS STRING), ''),
# MAGIC             coalesce(cast(`Muscle mass (kg)` AS STRING), ''),
# MAGIC             coalesce(cast(`Hydration (kg)` AS STRING), ''),
# MAGIC             coalesce(Comments, '')
# MAGIC         ), 256
# MAGIC     ) AS row_hash,
# MAGIC     current_timestamp() AS load_datetime
# MAGIC FROM deduped_weight
# MAGIC WHERE rn = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO health_dw.silver.weight AS target
# MAGIC USING health_dw.silver.weight_staging AS source
# MAGIC ON target.business_key_hash = source.business_key_hash
# MAGIC
# MAGIC WHEN MATCHED AND target.row_hash <> source.row_hash THEN
# MAGIC   UPDATE SET
# MAGIC     target.sk_date         = source.sk_date,
# MAGIC     target.sk_time         = source.sk_time,
# MAGIC     target.datetime        = source.datetime,
# MAGIC     target.weight_kg       = source.weight_kg,
# MAGIC     target.fat_mass_kg     = source.fat_mass_kg,
# MAGIC     target.bone_mass_kg    = source.bone_mass_kg,
# MAGIC     target.muscle_mass_kg  = source.muscle_mass_kg,
# MAGIC     target.hydration_kg    = source.hydration_kg,
# MAGIC     target.row_hash        = source.row_hash,
# MAGIC     target.update_datetime = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     sk_date, sk_time, datetime, weight_kg, fat_mass_kg, bone_mass_kg,
# MAGIC     muscle_mass_kg, hydration_kg, business_key_hash, row_hash, load_datetime, update_datetime
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.sk_date, source.sk_time, source.datetime, source.weight_kg,
# MAGIC     source.fat_mass_kg, source.bone_mass_kg, source.muscle_mass_kg, source.hydration_kg,
# MAGIC     source.business_key_hash, source.row_hash, current_timestamp(), current_timestamp()
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS health_dw.silver.weight_staging;
