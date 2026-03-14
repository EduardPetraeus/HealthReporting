-- merge_lifesum_bodyfat.sql
-- Per-source merge: Lifesum body fat CSV export -> silver.body_fat
-- Business key: date || 'lifesum' (one measurement per day from Lifesum)
--
-- Usage: python run_merge.py silver/merge_lifesum_bodyfat.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.body_fat__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY date
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_lifesum_bodyfat
    WHERE date IS NOT NULL
)
SELECT
    -- Surrogate date key
    (year(date::DATE) * 10000 + month(date::DATE) * 100 + day(date::DATE))::INTEGER AS sk_date,

    -- Business columns
    date::DATE                      AS day,
    bodyfat_pct::DOUBLE             AS body_fat_pct,

    -- Deterministic business key hash (includes source for future multi-source support)
    md5(coalesce(date, '') || '||lifesum') AS business_key_hash,

    -- Row hash (change detection)
    md5(
        coalesce(cast(bodyfat_pct AS VARCHAR), '')
    ) AS row_hash,

    current_timestamp AS load_datetime

FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.body_fat
MERGE INTO silver.body_fat AS target
USING silver.body_fat__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date         = src.sk_date,
    day             = src.day,
    body_fat_pct    = src.body_fat_pct,
    row_hash        = src.row_hash,
    update_datetime = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, day, body_fat_pct,
    business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.day, src.body_fat_pct,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.body_fat__staging;
