-- merge_apple_health_body_measurement.sql
-- Per-source merge: Apple Health -> silver.body_measurement
-- Wide JOIN of body composition metrics on date + sourceName
-- Shared table: source_name for future Withings data (A7)
--
-- Sources:
--   bronze.stg_apple_health_body_mass (weight in kg)
--   bronze.stg_apple_health_body_fat_percentage (body fat %)
--   bronze.stg_apple_health_lean_body_mass (lean mass in kg)
--   bronze.stg_apple_health_bmi (BMI)
--   bronze.stg_apple_health_height (height in m)
--
-- Usage: python run_merge.py silver/merge_apple_health_body_measurement.sql

-- Step 1: Aggregate each source to daily level, then wide JOIN
CREATE OR REPLACE TABLE silver.body_measurement__staging AS
WITH mass_daily AS (
    SELECT startDate::DATE AS date, sourceName AS source_name,
           value::DOUBLE AS weight_kg,
           ROW_NUMBER() OVER (PARTITION BY startDate::DATE, sourceName ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_apple_health_body_mass
    WHERE startDate IS NOT NULL AND value::DOUBLE > 0
),
fat_daily AS (
    SELECT startDate::DATE AS date, sourceName AS source_name,
           value::DOUBLE AS body_fat_pct,
           ROW_NUMBER() OVER (PARTITION BY startDate::DATE, sourceName ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_apple_health_body_fat_percentage
    WHERE startDate IS NOT NULL AND value::DOUBLE > 0
),
lean_daily AS (
    SELECT startDate::DATE AS date, sourceName AS source_name,
           value::DOUBLE AS lean_body_mass_kg,
           ROW_NUMBER() OVER (PARTITION BY startDate::DATE, sourceName ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_apple_health_lean_body_mass
    WHERE startDate IS NOT NULL AND value::DOUBLE > 0
),
bmi_daily AS (
    SELECT startDate::DATE AS date, sourceName AS source_name,
           value::DOUBLE AS bmi,
           ROW_NUMBER() OVER (PARTITION BY startDate::DATE, sourceName ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_apple_health_bmi
    WHERE startDate IS NOT NULL AND value::DOUBLE > 0
),
height_daily AS (
    SELECT startDate::DATE AS date, sourceName AS source_name,
           value::DOUBLE AS height_m,
           ROW_NUMBER() OVER (PARTITION BY startDate::DATE, sourceName ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_apple_health_height
    WHERE startDate IS NOT NULL AND value::DOUBLE > 0
),
combined AS (
    SELECT
        coalesce(m.date, f.date, l.date, b.date, h.date) AS date,
        coalesce(m.source_name, f.source_name, l.source_name, b.source_name, h.source_name) AS source_name,
        m.weight_kg,
        f.body_fat_pct,
        l.lean_body_mass_kg,
        b.bmi,
        h.height_m
    FROM (SELECT date, source_name, weight_kg FROM mass_daily WHERE rn = 1) m
    FULL OUTER JOIN (SELECT date, source_name, body_fat_pct FROM fat_daily WHERE rn = 1) f
        ON m.date = f.date AND m.source_name = f.source_name
    FULL OUTER JOIN (SELECT date, source_name, lean_body_mass_kg FROM lean_daily WHERE rn = 1) l
        ON coalesce(m.date, f.date) = l.date AND coalesce(m.source_name, f.source_name) = l.source_name
    FULL OUTER JOIN (SELECT date, source_name, bmi FROM bmi_daily WHERE rn = 1) b
        ON coalesce(m.date, f.date, l.date) = b.date AND coalesce(m.source_name, f.source_name, l.source_name) = b.source_name
    FULL OUTER JOIN (SELECT date, source_name, height_m FROM height_daily WHERE rn = 1) h
        ON coalesce(m.date, f.date, l.date, b.date) = h.date AND coalesce(m.source_name, f.source_name, l.source_name, b.source_name) = h.source_name
)
SELECT
    (year(date) * 10000 + month(date) * 100 + day(date))::INTEGER AS sk_date,
    date,
    ROUND(weight_kg, 2) AS weight_kg,
    ROUND(body_fat_pct, 2) AS body_fat_pct,
    ROUND(lean_body_mass_kg, 2) AS lean_body_mass_kg,
    ROUND(bmi, 2) AS bmi,
    ROUND(height_m, 3) AS height_m,
    source_name,
    md5(
        coalesce(cast(date AS VARCHAR), '') || '||' || coalesce(source_name, '')
    ) AS business_key_hash,
    md5(
        coalesce(cast(weight_kg AS VARCHAR), '') || '||' ||
        coalesce(cast(body_fat_pct AS VARCHAR), '') || '||' ||
        coalesce(cast(lean_body_mass_kg AS VARCHAR), '') || '||' ||
        coalesce(cast(bmi AS VARCHAR), '') || '||' ||
        coalesce(cast(height_m AS VARCHAR), '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM combined;

-- Step 2: Merge staging into silver.body_measurement
MERGE INTO silver.body_measurement AS target
USING silver.body_measurement__staging AS src
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
    source_name        = src.source_name,
    row_hash           = src.row_hash,
    update_datetime    = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, date, weight_kg, body_fat_pct, lean_body_mass_kg, bmi, height_m,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.date, src.weight_kg, src.body_fat_pct, src.lean_body_mass_kg, src.bmi, src.height_m,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.body_measurement__staging;
