-- Databricks notebook source
-- MAGIC %md
-- MAGIC Build a core data model for my weight

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.silver.weight_staging AS
WITH deduped_weight AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY Date, `Weight (kg)`, `Fat mass (kg)`, `Bone mass (kg)`, `Muscle mass (kg)`, `Hydration (kg)`
            ORDER BY Date
        ) AS rn
    FROM workspace.default.weight
    WHERE Date IS NOT NULL
      AND `Weight (kg)` IS NOT NULL
)
SELECT
    year(Date) * 10000 + month(Date) * 100 + dayofmonth(Date) AS sk_date,
    lpad(hour(Date), 2, '0') || lpad(minute(Date), 2, '0') AS sk_time,
    Date AS datetime,
    `Weight (kg)` AS weight_kg,
    `Fat mass (kg)` AS fat_mass_kg,
    `Bone mass (kg)` AS bone_mass_kg,
    `Muscle mass (kg)` AS muscle_mass_kg,
    `Hydration (kg)` AS hydration_kg,
    sha2(
        concat_ws(
            '||',
            coalesce(cast(Date AS STRING), ''),
            coalesce(cast(`Weight (kg)` AS STRING), ''),
            coalesce(cast(`Fat mass (kg)` AS STRING), ''),
            coalesce(cast(`Bone mass (kg)` AS STRING), ''),
            coalesce(cast(`Muscle mass (kg)` AS STRING), ''),
            coalesce(cast(`Hydration (kg)` AS STRING), '')
        ),
        256
    ) AS business_key_hash,
    sha2(
        concat_ws(
            '||',
            coalesce(cast(`Weight (kg)` AS STRING), ''),
            coalesce(cast(`Fat mass (kg)` AS STRING), ''),
            coalesce(cast(`Bone mass (kg)` AS STRING), ''),
            coalesce(cast(`Muscle mass (kg)` AS STRING), ''),
            coalesce(cast(`Hydration (kg)` AS STRING), ''),
            coalesce(Comments, '')
        ),
        256
    ) AS row_hash,
    current_timestamp() AS load_datetime
FROM deduped_weight
WHERE rn = 1

-- COMMAND ----------

MERGE INTO workspace.silver.weight AS target
USING workspace.silver.weight_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED
  AND target.row_hash <> source.row_hash
THEN UPDATE SET
    target.sk_date         = source.sk_date,
    target.sk_time         = source.sk_time,
    target.datetime        = source.datetime,
    target.weight_kg       = source.weight_kg,
    target.fat_mass_kg     = source.fat_mass_kg,
    target.bone_mass_kg    = source.bone_mass_kg,
    target.muscle_mass_kg  = source.muscle_mass_kg,
    target.hydration_kg    = source.hydration_kg,
    target.row_hash        = source.row_hash,
    target.update_datetime = current_timestamp()

WHEN NOT MATCHED THEN
  INSERT (
      sk_date,
      sk_time,
      datetime,
      weight_kg,
      fat_mass_kg,
      bone_mass_kg,
      muscle_mass_kg,
      hydration_kg,
      business_key_hash,
      row_hash,
      load_datetime,
      update_datetime
  )
  VALUES (
      source.sk_date,
      source.sk_time,
      source.datetime,
      source.weight_kg,
      source.fat_mass_kg,
      source.bone_mass_kg,
      source.muscle_mass_kg,
      source.hydration_kg,
      source.business_key_hash,
      source.row_hash,
      current_timestamp(),
      current_timestamp()
  )

-- COMMAND ----------

DROP TABLE IF EXISTS workspace.silver.weight_staging;
