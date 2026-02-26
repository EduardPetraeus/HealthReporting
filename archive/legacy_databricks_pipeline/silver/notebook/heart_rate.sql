-- Databricks notebook source
-- MAGIC %md
-- MAGIC Build a core data model for my heart rate

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.heart_rate_staging AS
WITH deduped_heart_rate AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY timestamp, source
            ORDER BY timestamp
        ) AS rn
    FROM workspace.default.oura_heart_rate
    WHERE timestamp IS NOT NULL
)
SELECT
-- Surrogate date key
    year(timestamp) * 10000 + month(timestamp) * 100 + dayofmonth(timestamp) AS sk_date,
-- Surrogate time key
    lpad(hour(timestamp), 2, '0') || lpad(minute(timestamp), 2, '0') AS sk_time,
    
    timestamp,
    bpm,
    source,
    sha2(
        concat_ws(
            '||',
            coalesce(CAST(timestamp AS STRING), ''),
            coalesce(source, '')
        ),
        256
    ) AS business_key_hash,
    sha2(
        concat_ws(
            '||',
            CAST(timestamp AS STRING),
            CAST(bpm AS STRING),
            source
        ),
        256
    ) AS row_hash,
    current_timestamp() AS load_datetime
FROM deduped_heart_rate
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO health_dw.silver.heart_rate AS target
USING health_dw.silver.heart_rate_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
  UPDATE SET
    target.sk_date           = source.sk_date,
    target.sk_time           = source.sk_time,
    target.timestamp         = source.timestamp,
    target.bpm               = source.bpm,
    target.source            = source.source,
    target.business_key_hash = source.business_key_hash,
    target.row_hash          = source.row_hash,
    target.update_datetime   = current_timestamp()

WHEN NOT MATCHED THEN
  INSERT (
    sk_date,
    sk_time,
    timestamp,
    bpm,
    source,
    business_key_hash,
    row_hash,
    load_datetime,
    update_datetime
  )
  VALUES (
    source.sk_date,
    source.sk_time,
    source.timestamp,
    source.bpm,
    source.source,
    source.business_key_hash,
    source.row_hash,
    current_timestamp(),
    current_timestamp()
  );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.heart_rate_staging;
