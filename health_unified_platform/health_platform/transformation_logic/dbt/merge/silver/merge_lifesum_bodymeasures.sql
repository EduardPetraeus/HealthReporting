-- merge_lifesum_bodymeasures.sql
-- Per-source merge: Lifesum body measures CSV export -> silver.body_measures
-- Business key: date (one measurement per day from Lifesum)
--
-- Usage: python run_merge.py silver/merge_lifesum_bodymeasures.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.body_measures__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_lifesum_bodymeasures
    WHERE date IS NOT NULL
)
SELECT
    -- Surrogate date key
    (year(date::DATE) * 10000 + month(date::DATE) * 100 + day(date::DATE))::INTEGER AS sk_date,

    -- Business columns
    date::DATE              AS day,
    weight_kg::DOUBLE       AS weight_kg,
    body_fat_pct::DOUBLE    AS body_fat_pct,
    muscle_mass_pct::DOUBLE AS muscle_mass_pct,
    waist_cm::DOUBLE        AS waist_cm,
    hip_cm::DOUBLE          AS hip_cm,
    chest_cm::DOUBLE        AS chest_cm,
    'lifesum'               AS source_system,

    -- Deterministic business key hash (identity: one row per date)
    md5(coalesce(date, '')) AS business_key_hash,

    -- Row hash (change detection on all measurable values)
    md5(
        coalesce(cast(weight_kg AS VARCHAR), '')       || '||' ||
        coalesce(cast(body_fat_pct AS VARCHAR), '')    || '||' ||
        coalesce(cast(muscle_mass_pct AS VARCHAR), '') || '||' ||
        coalesce(cast(waist_cm AS VARCHAR), '')        || '||' ||
        coalesce(cast(hip_cm AS VARCHAR), '')          || '||' ||
        coalesce(cast(chest_cm AS VARCHAR), '')
    ) AS row_hash,

    current_timestamp AS load_datetime

FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.body_measures
MERGE INTO silver.body_measures AS target
USING silver.body_measures__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date          = src.sk_date,
    day              = src.day,
    weight_kg        = src.weight_kg,
    body_fat_pct     = src.body_fat_pct,
    muscle_mass_pct  = src.muscle_mass_pct,
    waist_cm         = src.waist_cm,
    hip_cm           = src.hip_cm,
    chest_cm         = src.chest_cm,
    source_system    = src.source_system,
    row_hash         = src.row_hash,
    update_datetime  = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, day, weight_kg, body_fat_pct, muscle_mass_pct,
    waist_cm, hip_cm, chest_cm, source_system,
    business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.day, src.weight_kg, src.body_fat_pct, src.muscle_mass_pct,
    src.waist_cm, src.hip_cm, src.chest_cm, src.source_system,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.body_measures__staging;
