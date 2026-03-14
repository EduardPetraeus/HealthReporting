-- merge_withings_weight.sql
-- Per-source merge: Withings API + CSV -> silver.weight
-- UNION ALL from API (stg_withings_weight) and CSV (stg_withings_weight_csv)
-- Business key: datetime + weight_kg (composite uniqueness)
--
-- Usage: python run_merge.py silver/merge_withings_weight.sql

CREATE OR REPLACE TABLE silver.weight__staging AS
WITH api_source AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY datetime, weight_kg ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_weight
    WHERE datetime IS NOT NULL
),
csv_source AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY "Date" ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_withings_weight_csv
    WHERE "Date" IS NOT NULL
),
combined AS (
    SELECT
        datetime::TIMESTAMP               AS datetime,
        weight_kg::DOUBLE                 AS weight_kg,
        fat_mass_kg::DOUBLE               AS fat_mass_kg,
        bone_mass_kg::DOUBLE              AS bone_mass_kg,
        muscle_mass_kg::DOUBLE            AS muscle_mass_kg,
        hydration_kg::DOUBLE              AS hydration_kg
    FROM api_source WHERE rn = 1

    UNION ALL

    SELECT
        "Date"::TIMESTAMP                                AS datetime,
        TRY_CAST("Weight (kg)" AS DOUBLE)                AS weight_kg,
        TRY_CAST("Fat mass (kg)" AS DOUBLE)              AS fat_mass_kg,
        TRY_CAST("Bone mass (kg)" AS DOUBLE)             AS bone_mass_kg,
        TRY_CAST("Muscle mass (kg)" AS DOUBLE)           AS muscle_mass_kg,
        TRY_CAST("Hydration (kg)" AS DOUBLE)             AS hydration_kg
    FROM csv_source WHERE rn = 1
),
final_dedup AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY datetime ORDER BY weight_kg DESC NULLS LAST) AS rn
    FROM combined
    WHERE datetime IS NOT NULL
)
SELECT
    (year(datetime) * 10000 + month(datetime) * 100 + day(datetime))::INTEGER AS sk_date,
    lpad(hour(datetime)::VARCHAR, 2, '0') || lpad(minute(datetime)::VARCHAR, 2, '0') AS sk_time,
    datetime,
    ROUND(weight_kg, 2) AS weight_kg,
    ROUND(fat_mass_kg, 2) AS fat_mass_kg,
    ROUND(bone_mass_kg, 2) AS bone_mass_kg,
    ROUND(muscle_mass_kg, 2) AS muscle_mass_kg,
    ROUND(hydration_kg, 2) AS hydration_kg,
    md5(
        coalesce(cast(datetime AS VARCHAR), '') || '||' ||
        coalesce(cast(weight_kg AS VARCHAR), '')
    ) AS business_key_hash,
    md5(
        coalesce(cast(datetime AS VARCHAR), '') || '||' ||
        coalesce(cast(weight_kg AS VARCHAR), '') || '||' ||
        coalesce(cast(fat_mass_kg AS VARCHAR), '') || '||' ||
        coalesce(cast(bone_mass_kg AS VARCHAR), '') || '||' ||
        coalesce(cast(muscle_mass_kg AS VARCHAR), '') || '||' ||
        coalesce(cast(hydration_kg AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM final_dedup WHERE rn = 1;

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
