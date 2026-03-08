-- merge_withings_body_measurement.sql
-- Per-source merge: Withings CSV -> silver.body_measurement (shared table)
-- Source-qualified staging table to avoid collisions with Apple Health merge.
-- Withings provides fat_mass_kg, bone_mass_kg, muscle_mass_kg, hydration_kg.
-- Body fat % is computed from fat_mass_kg / weight_kg.
--
-- Usage: python run_merge.py silver/merge_withings_body_measurement.sql

-- Step 1: Create staging table with deduplication (daily grain)
CREATE OR REPLACE TABLE silver.body_measurement__withings_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY "Date"::TIMESTAMP::DATE
            ORDER BY "Date" DESC, _ingested_at DESC
        ) AS rn
    FROM bronze.stg_withings_weight
    WHERE "Date" IS NOT NULL
)
SELECT
    (year("Date"::TIMESTAMP::DATE) * 10000 + month("Date"::TIMESTAMP::DATE) * 100 + day("Date"::TIMESTAMP::DATE))::INTEGER AS sk_date,
    "Date"::TIMESTAMP::DATE AS date,
    ROUND(TRY_CAST("Weight (kg)" AS DOUBLE), 2) AS weight_kg,
    CASE WHEN TRY_CAST("Fat mass (kg)" AS DOUBLE) IS NOT NULL
              AND TRY_CAST("Weight (kg)" AS DOUBLE) IS NOT NULL
              AND TRY_CAST("Weight (kg)" AS DOUBLE) > 0
         THEN ROUND(TRY_CAST("Fat mass (kg)" AS DOUBLE) / TRY_CAST("Weight (kg)" AS DOUBLE) * 100, 2)
         ELSE NULL END AS body_fat_pct,
    NULL::DOUBLE AS lean_body_mass_kg,
    NULL::DOUBLE AS bmi,
    NULL::DOUBLE AS height_m,
    ROUND(TRY_CAST("Fat mass (kg)" AS DOUBLE), 2) AS fat_mass_kg,
    ROUND(TRY_CAST("Bone mass (kg)" AS DOUBLE), 2) AS bone_mass_kg,
    ROUND(TRY_CAST("Muscle mass (kg)" AS DOUBLE), 2) AS muscle_mass_kg,
    ROUND(TRY_CAST("Hydration (kg)" AS DOUBLE), 2) AS hydration_kg,
    'withings' AS source_name,
    md5(
        coalesce(cast("Date"::TIMESTAMP::DATE AS VARCHAR), '') || '||' || 'withings'
    ) AS business_key_hash,
    md5(
        coalesce(cast("Weight (kg)" AS VARCHAR), '') || '||' ||
        coalesce(cast("Fat mass (kg)" AS VARCHAR), '') || '||' ||
        coalesce(cast("Bone mass (kg)" AS VARCHAR), '') || '||' ||
        coalesce(cast("Muscle mass (kg)" AS VARCHAR), '') || '||' ||
        coalesce(cast("Hydration (kg)" AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.body_measurement
MERGE INTO silver.body_measurement AS target
USING silver.body_measurement__withings_staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date            = src.sk_date,
    date               = src.date,
    weight_kg          = src.weight_kg,
    body_fat_pct       = src.body_fat_pct,
    lean_body_mass_kg  = src.lean_body_mass_kg,
    bmi                = src.bmi,
    height_m           = src.height_m,
    fat_mass_kg        = src.fat_mass_kg,
    bone_mass_kg       = src.bone_mass_kg,
    muscle_mass_kg     = src.muscle_mass_kg,
    hydration_kg       = src.hydration_kg,
    source_name        = src.source_name,
    row_hash           = src.row_hash,
    update_datetime    = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, date, weight_kg, body_fat_pct, lean_body_mass_kg, bmi, height_m,
    fat_mass_kg, bone_mass_kg, muscle_mass_kg, hydration_kg,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.date, src.weight_kg, src.body_fat_pct, src.lean_body_mass_kg, src.bmi, src.height_m,
    src.fat_mass_kg, src.bone_mass_kg, src.muscle_mass_kg, src.hydration_kg,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.body_measurement__withings_staging;
