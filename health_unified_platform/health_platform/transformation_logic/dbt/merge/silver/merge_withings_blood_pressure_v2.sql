-- merge_withings_blood_pressure_v2.sql
-- Per-source merge: Withings CSV -> silver.blood_pressure_v2 (shared table)
-- Source-qualified staging table to avoid collisions with Apple Health merge.
-- Withings provides pulse (heart rate) in addition to systolic/diastolic.
--
-- Usage: python run_merge.py silver/merge_withings_blood_pressure_v2.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.blood_pressure_v2__withings_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY "Date"
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_withings_blood_pressure
    WHERE "Systolic" IS NOT NULL
)
SELECT
    (year("Date"::TIMESTAMP::DATE) * 10000 + month("Date"::TIMESTAMP::DATE) * 100 + day("Date"::TIMESTAMP::DATE))::INTEGER AS sk_date,
    lpad(hour("Date"::TIMESTAMP)::VARCHAR, 2, '0') || lpad(minute("Date"::TIMESTAMP)::VARCHAR, 2, '0') AS sk_time,
    "Date"::TIMESTAMP AS timestamp,
    ROUND("Systolic"::DOUBLE, 1) AS systolic_mmhg,
    ROUND("Diastolic"::DOUBLE, 1) AS diastolic_mmhg,
    ROUND("Heart rate"::DOUBLE, 1) AS pulse_bpm,
    'withings' AS source_name,
    md5(
        coalesce(cast("Date" AS VARCHAR), '') || '||' || 'withings'
    ) AS business_key_hash,
    md5(
        coalesce(cast("Date" AS VARCHAR), '') || '||' ||
        coalesce(cast("Systolic" AS VARCHAR), '') || '||' ||
        coalesce(cast("Diastolic" AS VARCHAR), '') || '||' ||
        coalesce(cast("Heart rate" AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.blood_pressure_v2
MERGE INTO silver.blood_pressure_v2 AS target
USING silver.blood_pressure_v2__withings_staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    sk_time           = src.sk_time,
    timestamp         = src.timestamp,
    systolic_mmhg     = src.systolic_mmhg,
    diastolic_mmhg    = src.diastolic_mmhg,
    pulse_bpm         = src.pulse_bpm,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, sk_time, timestamp, systolic_mmhg, diastolic_mmhg, pulse_bpm,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.sk_time, src.timestamp, src.systolic_mmhg, src.diastolic_mmhg, src.pulse_bpm,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.blood_pressure_v2__withings_staging;
