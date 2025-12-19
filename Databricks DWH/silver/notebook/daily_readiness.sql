-- Databricks notebook source
-- MAGIC %md
-- MAGIC Build a core data model for the daily readiness

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.silver.daily_readiness_staging AS
WITH deduped_readiness AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY id
        ) AS rn
    FROM workspace.default.oura_daily_readiness
    WHERE id IS NOT NULL
)
SELECT
    -- Surrogate date key
    year(day) * 10000 + month(day) * 100 + dayofmonth(day) AS sk_date,

    -- Business columns
    id,
    day,
    score AS readiness_score,
    temperature_deviation,
    temperature_trend_deviation,
    timestamp,

    -- Contributors split from JSON
    CAST(get_json_object(contributors, '$.activity_balance') AS DOUBLE) AS activity_balance,
    CAST(get_json_object(contributors, '$.body_temperature') AS DOUBLE) AS body_temperature,
    CAST(get_json_object(contributors, '$.hrv_balance') AS DOUBLE) AS hrv_balance,
    CAST(get_json_object(contributors, '$.previous_day_activity') AS DOUBLE) AS previous_day_activity,
    CAST(get_json_object(contributors, '$.previous_night') AS DOUBLE) AS previous_night,
    CAST(get_json_object(contributors, '$.recovery_index') AS DOUBLE) AS recovery_index,
    CAST(get_json_object(contributors, '$.resting_heart_rate') AS DOUBLE) AS resting_heart_rate,
    CAST(get_json_object(contributors, '$.sleep_balance') AS DOUBLE) AS sleep_balance,
    CAST(get_json_object(contributors, '$.sleep_regularity') AS DOUBLE) AS sleep_regularity,

    -- Row hash (change detection)
    sha2(
        concat_ws(
            '||',
            score,
            temperature_deviation,
            temperature_trend_deviation,
            get_json_object(contributors, '$.activity_balance'),
            get_json_object(contributors, '$.body_temperature'),
            get_json_object(contributors, '$.hrv_balance'),
            get_json_object(contributors, '$.previous_day_activity'),
            get_json_object(contributors, '$.previous_night'),
            get_json_object(contributors, '$.recovery_index'),
            get_json_object(contributors, '$.resting_heart_rate'),
            get_json_object(contributors, '$.sleep_balance'),
            get_json_object(contributors, '$.sleep_regularity')
        ),
        256
    ) AS row_hash,

    current_timestamp() AS load_datetime
FROM deduped_readiness
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO workspace.silver.daily_readiness AS target
USING workspace.silver.daily_readiness_staging AS source
ON target.id = source.id AND target.day = source.day

WHEN MATCHED
  AND target.row_hash <> source.row_hash
THEN UPDATE SET
    target.sk_date                    = source.sk_date,
    target.id                         = source.id,
    target.day                        = source.day,
    target.readiness_score            = source.readiness_score,
    target.temperature_deviation      = source.temperature_deviation,
    target.temperature_trend_deviation= source.temperature_trend_deviation,
    target.timestamp                  = source.timestamp,
    target.activity_balance           = source.activity_balance,
    target.body_temperature           = source.body_temperature,
    target.hrv_balance                = source.hrv_balance,
    target.previous_day_activity      = source.previous_day_activity,
    target.previous_night             = source.previous_night,
    target.recovery_index             = source.recovery_index,
    target.resting_heart_rate         = source.resting_heart_rate,
    target.sleep_balance              = source.sleep_balance,
    target.sleep_regularity           = source.sleep_regularity,
    target.row_hash                   = source.row_hash,
    target.update_datetime            = current_timestamp()

WHEN NOT MATCHED THEN
  INSERT (
      sk_date,
      id,
      day,
      readiness_score,
      temperature_deviation,
      temperature_trend_deviation,
      timestamp,
      activity_balance,
      body_temperature,
      hrv_balance,
      previous_day_activity,
      previous_night,
      recovery_index,
      resting_heart_rate,
      sleep_balance,
      sleep_regularity,
      row_hash,
      load_datetime,
      update_datetime
  )
  VALUES (
      source.sk_date,
      source.id,
      source.day,
      source.readiness_score,
      source.temperature_deviation,
      source.temperature_trend_deviation,
      source.timestamp,
      source.activity_balance,
      source.body_temperature,
      source.hrv_balance,
      source.previous_day_activity,
      source.previous_night,
      source.recovery_index,
      source.resting_heart_rate,
      source.sleep_balance,
      source.sleep_regularity,
      source.row_hash,
      current_timestamp(),
      current_timestamp()
  );

-- COMMAND ----------

DROP TABLE IF EXISTS workspace.silver.daily_readiness_staging;
