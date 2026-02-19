-- merge_oura_personal_info.sql
-- Per-source merge: Oura API -> silver.personal_info
-- Business key: Oura user id (single row — slowly changing)
--
-- Usage: python run_merge.py silver/merge_oura_personal_info.sql

CREATE OR REPLACE TABLE silver.personal_info__staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_oura_personal_info
    WHERE id IS NOT NULL
)
SELECT
    age::INTEGER          AS age,
    weight::DOUBLE        AS weight_kg,
    height::DOUBLE        AS height_m,
    biological_sex,
    email,
    md5(coalesce(id, '')) AS business_key_hash,
    md5(
        coalesce(cast(age AS VARCHAR), '')    || '||' ||
        coalesce(cast(weight AS VARCHAR), '') || '||' ||
        coalesce(cast(height AS VARCHAR), '') || '||' ||
        coalesce(biological_sex, '')          || '||' ||
        coalesce(email, '')
    )                     AS row_hash,
    current_timestamp     AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.personal_info AS target
USING silver.personal_info__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    age             = src.age,
    weight_kg       = src.weight_kg,
    height_m        = src.height_m,
    biological_sex  = src.biological_sex,
    email           = src.email,
    row_hash        = src.row_hash,
    update_datetime = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    age, weight_kg, height_m, biological_sex, email,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.age, src.weight_kg, src.height_m, src.biological_sex, src.email,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.personal_info__staging;
