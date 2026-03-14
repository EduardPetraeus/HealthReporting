-- merge_withings_blood_pressure.sql
-- Per-source merge: Withings API + CSV -> silver.blood_pressure
-- UNION ALL from API (stg_withings_blood_pressure) and CSV (stg_withings_blood_pressure_csv)
-- Business key: datetime + systolic + diastolic
--
-- Usage: python run_merge.py silver/merge_withings_blood_pressure.sql

CREATE OR REPLACE TABLE silver.blood_pressure__staging AS
WITH api_source AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY datetime ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_blood_pressure
    WHERE datetime IS NOT NULL
),
csv_source AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY "Date" ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_blood_pressure_csv
    WHERE "Date" IS NOT NULL
),
combined AS (
    SELECT
        datetime::TIMESTAMP               AS datetime,
        systolic_mmhg::INTEGER            AS systolic,
        diastolic_mmhg::INTEGER           AS diastolic,
        pulse_bpm::INTEGER                AS pulse
    FROM api_source WHERE rn = 1

    UNION ALL

    SELECT
        "Date"::TIMESTAMP                           AS datetime,
        TRY_CAST("Systolic" AS INTEGER)             AS systolic,
        TRY_CAST("Diastolic" AS INTEGER)            AS diastolic,
        TRY_CAST("Heart rate" AS INTEGER)           AS pulse
    FROM csv_source WHERE rn = 1
),
final_dedup AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY datetime, systolic, diastolic ORDER BY pulse DESC NULLS LAST) AS rn
    FROM combined
    WHERE datetime IS NOT NULL
)
SELECT
    (year(datetime) * 10000 + month(datetime) * 100 + day(datetime))::INTEGER AS sk_date,
    lpad(hour(datetime)::VARCHAR, 2, '0') || lpad(minute(datetime)::VARCHAR, 2, '0') AS sk_time,
    datetime,
    systolic,
    diastolic,
    pulse,
    md5(
        coalesce(cast(datetime AS VARCHAR), '') || '||' ||
        coalesce(cast(systolic AS VARCHAR), '') || '||' ||
        coalesce(cast(diastolic AS VARCHAR), '')
    ) AS business_key_hash,
    md5(
        coalesce(cast(datetime AS VARCHAR), '') || '||' ||
        coalesce(cast(systolic AS VARCHAR), '') || '||' ||
        coalesce(cast(diastolic AS VARCHAR), '') || '||' ||
        coalesce(cast(pulse AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM final_dedup WHERE rn = 1;

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
