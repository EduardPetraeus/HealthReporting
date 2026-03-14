-- merge_withings_pulse_wave_velocity.sql
-- Per-source merge: Withings CSV -> silver.pulse_wave_velocity
-- Follows the staging + MERGE + drop pattern, adapted for DuckDB.
--
-- Usage: python run_merge.py silver/merge_withings_pulse_wave_velocity.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.pulse_wave_velocity__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_withings_pwv
    WHERE date IS NOT NULL
)
SELECT
    (year(date::TIMESTAMP::DATE) * 10000 + month(date::TIMESTAMP::DATE) * 100 + day(date::TIMESTAMP::DATE))::INTEGER AS sk_date,
    date::TIMESTAMP AS timestamp,
    ROUND(TRY_CAST(value AS DOUBLE), 2) AS pwv_m_per_s,
    'withings' AS source_name,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' || 'withings'
    ) AS business_key_hash,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' ||
        coalesce(cast(value AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.pulse_wave_velocity
MERGE INTO silver.pulse_wave_velocity AS target
USING silver.pulse_wave_velocity__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    timestamp         = src.timestamp,
    pwv_m_per_s       = src.pwv_m_per_s,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, timestamp, pwv_m_per_s,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.timestamp, src.pwv_m_per_s,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.pulse_wave_velocity__staging;
