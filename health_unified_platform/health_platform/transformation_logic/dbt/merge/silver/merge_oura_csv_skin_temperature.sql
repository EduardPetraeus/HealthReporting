-- merge_oura_csv_skin_temperature.sql
-- Per-source merge: Oura CSV -> silver.skin_temperature
-- Aggregates intraday temperature samples to daily level.
--
-- Usage: python run_merge.py silver/merge_oura_csv_skin_temperature.sql

-- Step 1: Create staging table with daily aggregation
CREATE OR REPLACE TABLE silver.skin_temperature__staging AS
WITH daily AS (
    SELECT
        timestamp::TIMESTAMP::DATE AS date,
        ROUND(AVG(TRY_CAST(temperature AS DOUBLE)), 2) AS avg_skin_temp,
        ROUND(MIN(TRY_CAST(temperature AS DOUBLE)), 2) AS min_skin_temp,
        ROUND(MAX(TRY_CAST(temperature AS DOUBLE)), 2) AS max_skin_temp,
        COUNT(*) AS sample_count
    FROM bronze.stg_oura_csv_temperature
    WHERE timestamp IS NOT NULL
    GROUP BY timestamp::TIMESTAMP::DATE
)
SELECT
    (year(date) * 10000 + month(date) * 100 + day(date))::INTEGER AS sk_date,
    date,
    avg_skin_temp,
    min_skin_temp,
    max_skin_temp,
    sample_count,
    'oura' AS source_name,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' || 'oura'
    ) AS business_key_hash,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' ||
        coalesce(cast(avg_skin_temp AS VARCHAR), '') || '||' ||
        coalesce(cast(min_skin_temp AS VARCHAR), '') || '||' ||
        coalesce(cast(max_skin_temp AS VARCHAR), '') || '||' ||
        coalesce(cast(sample_count AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM daily;

-- Step 2: Merge staging into silver.skin_temperature
MERGE INTO silver.skin_temperature AS target
USING silver.skin_temperature__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    date              = src.date,
    avg_skin_temp     = src.avg_skin_temp,
    min_skin_temp     = src.min_skin_temp,
    max_skin_temp     = src.max_skin_temp,
    sample_count      = src.sample_count,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, date, avg_skin_temp, min_skin_temp, max_skin_temp, sample_count,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.date, src.avg_skin_temp, src.min_skin_temp, src.max_skin_temp, src.sample_count,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.skin_temperature__staging;
