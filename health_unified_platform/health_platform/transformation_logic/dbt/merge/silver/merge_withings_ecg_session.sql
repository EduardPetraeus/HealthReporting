-- merge_withings_ecg_session.sql
-- Per-source merge: Withings CSV -> silver.ecg_session
-- Follows the staging + MERGE + drop pattern, adapted for DuckDB.
--
-- Usage: python run_merge.py silver/merge_withings_ecg_session.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.ecg_session__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_withings_signal
    WHERE date IS NOT NULL
)
SELECT
    (year(date::TIMESTAMP::DATE) * 10000 + month(date::TIMESTAMP::DATE) * 100 + day(date::TIMESTAMP::DATE))::INTEGER AS sk_date,
    date::TIMESTAMP AS timestamp,
    type AS ecg_type,
    TRY_CAST("Frequency (Hz)" AS INTEGER) AS frequency_hz,
    TRY_CAST("Duration (s)" AS INTEGER) AS duration_s,
    wearposition,
    'withings' AS source_name,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' || 'withings'
    ) AS business_key_hash,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' ||
        coalesce(type, '') || '||' ||
        coalesce(cast("Frequency (Hz)" AS VARCHAR), '') || '||' ||
        coalesce(cast("Duration (s)" AS VARCHAR), '') || '||' ||
        coalesce(wearposition, '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.ecg_session
MERGE INTO silver.ecg_session AS target
USING silver.ecg_session__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    timestamp         = src.timestamp,
    ecg_type          = src.ecg_type,
    frequency_hz      = src.frequency_hz,
    duration_s        = src.duration_s,
    wearposition      = src.wearposition,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, timestamp, ecg_type, frequency_hz, duration_s, wearposition,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.timestamp, src.ecg_type, src.frequency_hz, src.duration_s, src.wearposition,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.ecg_session__staging;
