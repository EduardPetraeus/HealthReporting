-- merge_lifesum_weighins.sql
-- Per-source merge: Lifesum weigh-ins CSV export -> silver.weight
-- Business key: date || 'lifesum' (one weigh-in per day from Lifesum)
--
-- Usage: python run_merge.py silver/merge_lifesum_weighins.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.weight__staging_lifesum AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_lifesum_weighins
    WHERE date IS NOT NULL
)
SELECT
    -- Surrogate date key
    (year(date::DATE) * 10000 + month(date::DATE) * 100 + day(date::DATE))::INTEGER AS sk_date,

    -- Time key: Lifesum CSV has no time, use '0000' as default
    '0000'                    AS sk_time,

    -- Business columns
    date::TIMESTAMP           AS datetime,
    weight_kg::DOUBLE         AS weight_kg,
    NULL::DOUBLE              AS fat_mass_kg,
    NULL::DOUBLE              AS bone_mass_kg,
    NULL::DOUBLE              AS muscle_mass_kg,
    NULL::DOUBLE              AS hydration_kg,

    -- Deterministic business key hash (includes source to avoid collisions with Withings)
    md5(coalesce(date, '') || '||lifesum') AS business_key_hash,

    -- Row hash (change detection)
    md5(
        coalesce(cast(weight_kg AS VARCHAR), '')
    ) AS row_hash,

    current_timestamp AS load_datetime

FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.weight
MERGE INTO silver.weight AS target
USING silver.weight__staging_lifesum AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
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

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, sk_time, datetime, weight_kg, fat_mass_kg, bone_mass_kg,
    muscle_mass_kg, hydration_kg,
    business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.sk_time, src.datetime, src.weight_kg, src.fat_mass_kg, src.bone_mass_kg,
    src.muscle_mass_kg, src.hydration_kg,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.weight__staging_lifesum;
