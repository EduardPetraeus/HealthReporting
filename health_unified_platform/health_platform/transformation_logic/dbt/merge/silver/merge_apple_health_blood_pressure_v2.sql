-- merge_apple_health_blood_pressure_v2.sql
-- Per-source merge: Apple Health -> silver.blood_pressure_v2
-- JOIN of systolic + diastolic on matching startDate
-- Shared table: Withings also writes to this table (A7)
--
-- Sources:
--   bronze.stg_apple_health_bp_systolic
--   bronze.stg_apple_health_bp_diastolic
--
-- Usage: python run_merge.py silver/merge_apple_health_blood_pressure_v2.sql

-- Step 1: JOIN systolic and diastolic on startDate
CREATE OR REPLACE TABLE silver.blood_pressure_v2__staging AS
WITH sys_deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY startDate ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_apple_health_bp_systolic WHERE startDate IS NOT NULL
),
dia_deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY startDate ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_apple_health_bp_diastolic WHERE startDate IS NOT NULL
),
combined AS (
    SELECT s.startDate, s.endDate, s.value::DOUBLE AS systolic_mmhg, d.value::DOUBLE AS diastolic_mmhg, s.sourceName
    FROM sys_deduped s INNER JOIN dia_deduped d ON s.startDate = d.startDate
    WHERE s.rn = 1 AND d.rn = 1
)
SELECT
    (year(startDate) * 10000 + month(startDate) * 100 + day(startDate))::INTEGER AS sk_date,
    lpad(hour(startDate)::VARCHAR, 2, '0') || lpad(minute(startDate)::VARCHAR, 2, '0') AS sk_time,
    startDate::TIMESTAMP AS timestamp,
    ROUND(systolic_mmhg, 1) AS systolic_mmhg,
    ROUND(diastolic_mmhg, 1) AS diastolic_mmhg,
    NULL::DOUBLE AS pulse_bpm,
    'apple_health' AS source_name,
    md5(coalesce(cast(startDate AS VARCHAR), '') || '||' || 'apple_health') AS business_key_hash,
    md5(cast(startDate AS VARCHAR) || '||' || cast(systolic_mmhg AS VARCHAR) || '||' || cast(diastolic_mmhg AS VARCHAR)) AS row_hash,
    current_timestamp AS load_datetime
FROM combined;

-- Step 2: Merge staging into silver.blood_pressure_v2
MERGE INTO silver.blood_pressure_v2 AS target
USING silver.blood_pressure_v2__staging AS src
ON target.business_key_hash = src.business_key_hash
WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET sk_date=src.sk_date, sk_time=src.sk_time, timestamp=src.timestamp,
    systolic_mmhg=src.systolic_mmhg, diastolic_mmhg=src.diastolic_mmhg, pulse_bpm=src.pulse_bpm,
    source_name=src.source_name, row_hash=src.row_hash, update_datetime=current_timestamp
WHEN NOT MATCHED THEN
  INSERT (sk_date, sk_time, timestamp, systolic_mmhg, diastolic_mmhg, pulse_bpm, source_name, business_key_hash, row_hash, load_datetime, update_datetime)
  VALUES (src.sk_date, src.sk_time, src.timestamp, src.systolic_mmhg, src.diastolic_mmhg, src.pulse_bpm, src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp);

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.blood_pressure_v2__staging;
