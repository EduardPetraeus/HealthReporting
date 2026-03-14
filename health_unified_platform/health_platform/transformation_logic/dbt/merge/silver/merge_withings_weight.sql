-- merge_withings_weight.sql
-- Per-source merge: Withings CSV -> silver.weight
-- Bronze columns use Withings CSV export names (quoted, with units).
-- Business key: Date + Weight (kg) (composite uniqueness)
--
-- Usage: python run_merge.py silver/merge_withings_weight.sql

CREATE OR REPLACE TABLE silver.weight__staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY "Date", "Weight (kg)" ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_weight
    WHERE "Date" IS NOT NULL
)
SELECT
    (year("Date"::TIMESTAMP) * 10000 + month("Date"::TIMESTAMP) * 100 + day("Date"::TIMESTAMP))::INTEGER AS sk_date,
    lpad(hour("Date"::TIMESTAMP)::VARCHAR, 2, '0') || lpad(minute("Date"::TIMESTAMP)::VARCHAR, 2, '0')   AS sk_time,
    "Date"::TIMESTAMP                  AS datetime,
    TRY_CAST("Weight (kg)" AS DOUBLE)  AS weight_kg,
    TRY_CAST("Fat mass (kg)" AS DOUBLE) AS fat_mass_kg,
    TRY_CAST("Bone mass (kg)" AS DOUBLE) AS bone_mass_kg,
    TRY_CAST("Muscle mass (kg)" AS DOUBLE) AS muscle_mass_kg,
    TRY_CAST("Hydration (kg)" AS DOUBLE) AS hydration_kg,
    md5(
        coalesce(cast("Date" AS VARCHAR), '')              || '||' ||
        coalesce(cast("Weight (kg)" AS VARCHAR), '')       || '||' ||
        coalesce(cast("Fat mass (kg)" AS VARCHAR), '')     || '||' ||
        coalesce(cast("Bone mass (kg)" AS VARCHAR), '')    || '||' ||
        coalesce(cast("Muscle mass (kg)" AS VARCHAR), '')  || '||' ||
        coalesce(cast("Hydration (kg)" AS VARCHAR), '')
    )                                  AS business_key_hash,
    md5(
        coalesce(cast("Date" AS VARCHAR), '')              || '||' ||
        coalesce(cast("Weight (kg)" AS VARCHAR), '')       || '||' ||
        coalesce(cast("Fat mass (kg)" AS VARCHAR), '')     || '||' ||
        coalesce(cast("Bone mass (kg)" AS VARCHAR), '')    || '||' ||
        coalesce(cast("Muscle mass (kg)" AS VARCHAR), '')  || '||' ||
        coalesce(cast("Hydration (kg)" AS VARCHAR), '')
    )                                  AS row_hash,
    current_timestamp                  AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.weight AS target
USING silver.weight__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date         = src.sk_date,
    sk_time         = src.sk_time,
    datetime        = src.datetime,
    weight_kg       = src.weight_kg,
    fat_mass_kg     = src.fat_mass_kg,
    bone_mass_kg    = src.bone_mass_kg,
    muscle_mass_kg  = src.muscle_mass_kg,
    hydration_kg    = src.hydration_kg,
    row_hash        = src.row_hash,
    update_datetime = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, sk_time, datetime, weight_kg, fat_mass_kg, bone_mass_kg,
    muscle_mass_kg, hydration_kg, business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.sk_time, src.datetime, src.weight_kg, src.fat_mass_kg, src.bone_mass_kg,
    src.muscle_mass_kg, src.hydration_kg, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.weight__staging;
