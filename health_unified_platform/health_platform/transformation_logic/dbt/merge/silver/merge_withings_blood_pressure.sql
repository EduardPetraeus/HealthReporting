-- merge_withings_blood_pressure.sql
-- Per-source merge: Withings CSV -> silver.blood_pressure
-- Bronze columns use Withings CSV export names (quoted).
-- Business key: Date + Systolic + Diastolic (one reading per timestamp)
--
-- Usage: python run_merge.py silver/merge_withings_blood_pressure.sql

CREATE OR REPLACE TABLE silver.blood_pressure__staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY "Date" ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_blood_pressure
    WHERE "Date" IS NOT NULL
)
SELECT
    (year("Date"::TIMESTAMP) * 10000 + month("Date"::TIMESTAMP) * 100 + day("Date"::TIMESTAMP))::INTEGER AS sk_date,
    lpad(hour("Date"::TIMESTAMP)::VARCHAR, 2, '0') || lpad(minute("Date"::TIMESTAMP)::VARCHAR, 2, '0')   AS sk_time,
    "Date"::TIMESTAMP                  AS datetime,
    TRY_CAST("Systolic" AS INTEGER)    AS systolic,
    TRY_CAST("Diastolic" AS INTEGER)   AS diastolic,
    TRY_CAST("Heart rate" AS INTEGER)  AS pulse,
    md5(
        coalesce(cast("Date" AS VARCHAR), '')              || '||' ||
        coalesce(cast("Systolic" AS VARCHAR), '')          || '||' ||
        coalesce(cast("Diastolic" AS VARCHAR), '')
    )                                  AS business_key_hash,
    md5(
        coalesce(cast("Date" AS VARCHAR), '')              || '||' ||
        coalesce(cast("Systolic" AS VARCHAR), '')          || '||' ||
        coalesce(cast("Diastolic" AS VARCHAR), '')         || '||' ||
        coalesce(cast("Heart rate" AS VARCHAR), '')
    )                                  AS row_hash,
    current_timestamp                  AS load_datetime
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
