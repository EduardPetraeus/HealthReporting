-- merge_oura_heartrate.sql
-- Per-source merge: Oura API -> silver.heart_rate
-- Business key: timestamp + source_name (per-minute readings)
--
-- Usage: python run_merge.py silver/merge_oura_heartrate.sql

CREATE OR REPLACE TABLE silver.heart_rate__staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY timestamp, source ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_oura_heartrate
    WHERE timestamp IS NOT NULL
)
SELECT
    (year(timestamp::TIMESTAMP) * 10000 + month(timestamp::TIMESTAMP) * 100 + day(timestamp::TIMESTAMP))::INTEGER AS sk_date,
    lpad(hour(timestamp::TIMESTAMP)::VARCHAR, 2, '0') || lpad(minute(timestamp::TIMESTAMP)::VARCHAR, 2, '0')       AS sk_time,
    timestamp::TIMESTAMP              AS timestamp,
    bpm::INTEGER                      AS bpm,
    source                            AS source_name,
    md5(
        coalesce(timestamp, '') || '||' || coalesce(source, '')
    )                                 AS business_key_hash,
    md5(
        coalesce(timestamp, '')             || '||' ||
        coalesce(cast(bpm AS VARCHAR), '')  || '||' ||
        coalesce(source, '')
    )                                 AS row_hash,
    current_timestamp                 AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.heart_rate AS target
USING silver.heart_rate__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date         = src.sk_date,
    sk_time         = src.sk_time,
    timestamp       = src.timestamp,
    bpm             = src.bpm,
    source_name     = src.source_name,
    row_hash        = src.row_hash,
    update_datetime = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, sk_time, timestamp, bpm, source_name,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.sk_time, src.timestamp, src.bpm, src.source_name,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.heart_rate__staging;
