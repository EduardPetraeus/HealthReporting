-- merge_oura_blood_oxygen.sql
-- Per-source merge: Oura API -> silver.blood_oxygen
-- Business key: timestamp + source (per-minute continuous SpO2 readings)
--
-- Usage: python run_merge.py silver/merge_oura_blood_oxygen.sql

CREATE OR REPLACE TABLE silver.blood_oxygen__staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY timestamp, source ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_oura_blood_oxygen
    WHERE timestamp IS NOT NULL
)
SELECT
    (year(timestamp::TIMESTAMP) * 10000 + month(timestamp::TIMESTAMP) * 100 + day(timestamp::TIMESTAMP))::INTEGER AS sk_date,
    lpad(hour(timestamp::TIMESTAMP)::VARCHAR, 2, '0') || lpad(minute(timestamp::TIMESTAMP)::VARCHAR, 2, '0')       AS sk_time,
    timestamp::TIMESTAMP              AS timestamp,
    spo2::DOUBLE                      AS spo2,
    source                            AS source,
    md5(
        coalesce(timestamp, '') || '||' || coalesce(source, '')
    )                                 AS business_key_hash,
    md5(
        coalesce(timestamp, '')               || '||' ||
        coalesce(cast(spo2 AS VARCHAR), '')   || '||' ||
        coalesce(source, '')
    )                                 AS row_hash,
    current_timestamp                 AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.blood_oxygen AS target
USING silver.blood_oxygen__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date         = src.sk_date,
    sk_time         = src.sk_time,
    timestamp       = src.timestamp,
    spo2            = src.spo2,
    source          = src.source,
    row_hash        = src.row_hash,
    update_datetime = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, sk_time, timestamp, spo2, source,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.sk_time, src.timestamp, src.spo2, src.source,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.blood_oxygen__staging;
