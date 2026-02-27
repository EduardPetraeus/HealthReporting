-- merge_withings_blood_pressure.sql
-- Per-source merge: Withings -> silver.blood_pressure
-- Business key: datetime + systolic + diastolic (one reading per timestamp)
--
-- Usage: python run_merge.py silver/merge_withings_blood_pressure.sql

CREATE OR REPLACE TABLE silver.blood_pressure__staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY datetime ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_blood_pressure
    WHERE datetime IS NOT NULL
)
SELECT
    (year(datetime::TIMESTAMP) * 10000 + month(datetime::TIMESTAMP) * 100 + day(datetime::TIMESTAMP))::INTEGER AS sk_date,
    lpad(hour(datetime::TIMESTAMP)::VARCHAR, 2, '0') || lpad(minute(datetime::TIMESTAMP)::VARCHAR, 2, '0')      AS sk_time,
    datetime::TIMESTAMP               AS datetime,
    systolic::INTEGER                 AS systolic,
    diastolic::INTEGER                AS diastolic,
    pulse::INTEGER                    AS pulse,
    md5(
        coalesce(datetime, '')                             || '||' ||
        coalesce(cast(systolic AS VARCHAR), '')            || '||' ||
        coalesce(cast(diastolic AS VARCHAR), '')
    )                                 AS business_key_hash,
    md5(
        coalesce(datetime, '')                             || '||' ||
        coalesce(cast(systolic AS VARCHAR), '')            || '||' ||
        coalesce(cast(diastolic AS VARCHAR), '')           || '||' ||
        coalesce(cast(pulse AS VARCHAR), '')
    )                                 AS row_hash,
    current_timestamp                 AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.blood_pressure AS target
USING silver.blood_pressure__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date         = src.sk_date,
    sk_time         = src.sk_time,
    datetime        = src.datetime,
    systolic        = src.systolic,
    diastolic       = src.diastolic,
    pulse           = src.pulse,
    row_hash        = src.row_hash,
    update_datetime = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, sk_time, datetime, systolic, diastolic, pulse,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.sk_time, src.datetime, src.systolic, src.diastolic, src.pulse,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.blood_pressure__staging;
