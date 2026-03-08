-- merge_oura_csv_cardiovascular_age.sql
-- Per-source merge: Oura CSV -> silver.cardiovascular_age
-- Follows the staging + MERGE + drop pattern, adapted for DuckDB.
--
-- Usage: python run_merge.py silver/merge_oura_csv_cardiovascular_age.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.cardiovascular_age__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY day
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_oura_csv_dailycardiovascularage
    WHERE day IS NOT NULL
)
SELECT
    (year(day::DATE) * 10000 + month(day::DATE) * 100 + day(day::DATE))::INTEGER AS sk_date,
    day::DATE AS date,
    vascular_age::INTEGER AS vascular_age,
    'oura' AS source_name,
    md5(
        coalesce(cast(day AS VARCHAR), '') || '||' || 'oura'
    ) AS business_key_hash,
    md5(
        coalesce(cast(day AS VARCHAR), '') || '||' ||
        coalesce(cast(vascular_age AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.cardiovascular_age
MERGE INTO silver.cardiovascular_age AS target
USING silver.cardiovascular_age__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    date              = src.date,
    vascular_age      = src.vascular_age,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, date, vascular_age,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.date, src.vascular_age,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.cardiovascular_age__staging;
