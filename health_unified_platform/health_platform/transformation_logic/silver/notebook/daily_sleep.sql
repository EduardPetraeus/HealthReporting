-- Databricks notebook source
-- MAGIC %md
-- MAGIC Build a core data model for the daily sleep

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.daily_sleep_staging AS
WITH deduped_sleep AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id, day
            ORDER BY id, day
        ) AS rn
    FROM workspace.default.oura_daily_sleep
    WHERE id IS NOT NULL
)
SELECT
    -- Surrogate date key
    year(day) * 10000 + month(day) * 100 + dayofmonth(day) AS sk_date,

    -- Business columns
    id,
    day,
    score AS sleep_score,
    timestamp,

    -- Contributors split from JSON
    CAST(get_json_object(contributors, '$.deep_sleep') AS DOUBLE) AS deep_sleep,
    CAST(get_json_object(contributors, '$.efficiency') AS DOUBLE) AS efficiency,
    CAST(get_json_object(contributors, '$.latency') AS DOUBLE) AS latency,
    CAST(get_json_object(contributors, '$.rem_sleep') AS DOUBLE) AS rem_sleep,
    CAST(get_json_object(contributors, '$.restfulness') AS DOUBLE) AS restfulness,
    CAST(get_json_object(contributors, '$.timing') AS DOUBLE) AS timing,
    CAST(get_json_object(contributors, '$.total_sleep') AS DOUBLE) AS total_sleep,

    -- Row hash (change detection)
    sha2(
        concat_ws(
            '||',
            score,
            timestamp,
            get_json_object(contributors, '$.deep_sleep'),
            get_json_object(contributors, '$.efficiency'),
            get_json_object(contributors, '$.latency'),
            get_json_object(contributors, '$.rem_sleep'),
            get_json_object(contributors, '$.restfulness'),
            get_json_object(contributors, '$.timing'),
            get_json_object(contributors, '$.total_sleep')
        ),
        256
    ) AS row_hash,

    current_timestamp() AS load_datetime
FROM deduped_sleep
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO health_dw.silver.daily_sleep AS target
USING health_dw.silver.daily_sleep_staging AS source
ON target.id = source.id AND target.day = source.day

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
  UPDATE SET
    target.sk_date         = source.sk_date,
    target.sleep_score     = source.sleep_score,
    target.timestamp       = source.timestamp,
    target.deep_sleep      = source.deep_sleep,
    target.efficiency      = source.efficiency,
    target.latency         = source.latency,
    target.rem_sleep       = source.rem_sleep,
    target.restfulness     = source.restfulness,
    target.timing          = source.timing,
    target.total_sleep     = source.total_sleep,
    target.row_hash        = source.row_hash,
    target.update_datetime = current_timestamp()

WHEN NOT MATCHED THEN
  INSERT (
    sk_date,
    id,
    day,
    sleep_score,
    timestamp,
    deep_sleep,
    efficiency,
    latency,
    rem_sleep,
    restfulness,
    timing,
    total_sleep,
    row_hash,
    load_datetime,
    update_datetime
  )
  VALUES (
    source.sk_date,
    source.id,
    source.day,
    source.sleep_score,
    source.timestamp,
    source.deep_sleep,
    source.efficiency,
    source.latency,
    source.rem_sleep,
    source.restfulness,
    source.timing,
    source.total_sleep,
    source.row_hash,
    current_timestamp(),
    current_timestamp()
  );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.daily_sleep_staging;
