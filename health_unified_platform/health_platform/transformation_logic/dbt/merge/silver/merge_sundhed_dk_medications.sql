-- Per-source merge: sundhed.dk medications -> silver.sundhed_dk_medications
-- Business key: medication_name + start_date (one row per medication per start)
-- Source: bronze.stg_sundhed_dk_medications (parquet files from sundhed.dk HTML parser)
--
-- Usage: python run_merge.py silver/merge_sundhed_dk_medications.sql

CREATE OR REPLACE TABLE silver.sundhed_dk_medications__staging AS
WITH deduped AS (
    SELECT *,
        md5(
            coalesce(medication_name, '') || '||' ||
            coalesce(cast(start_date AS VARCHAR), '')
        )                                       AS business_key_hash,
        ROW_NUMBER() OVER (
            PARTITION BY md5(
                coalesce(medication_name, '') || '||' ||
                coalesce(cast(start_date AS VARCHAR), '')
            )
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_sundhed_dk_medications
    WHERE medication_name IS NOT NULL
)
SELECT
    medication_name,
    active_ingredient,
    strength,
    dosage,
    CAST(start_date AS DATE)                   AS start_date,
    CAST(end_date AS DATE)                     AS end_date,
    atc_code,
    reason,
    CASE
        WHEN end_date IS NULL OR CAST(end_date AS DATE) >= CURRENT_DATE THEN TRUE
        ELSE FALSE
    END                                        AS is_active,
    business_key_hash,
    md5(
        coalesce(active_ingredient, '')              || '||' ||
        coalesce(strength, '')                       || '||' ||
        coalesce(dosage, '')                         || '||' ||
        coalesce(cast(end_date AS VARCHAR), '')      || '||' ||
        coalesce(atc_code, '')                       || '||' ||
        coalesce(reason, '')
    )                                          AS row_hash,
    current_timestamp                          AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.sundhed_dk_medications AS target
USING silver.sundhed_dk_medications__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    medication_name      = src.medication_name,
    active_ingredient    = src.active_ingredient,
    strength             = src.strength,
    dosage               = src.dosage,
    start_date           = src.start_date,
    end_date             = src.end_date,
    atc_code             = src.atc_code,
    reason               = src.reason,
    is_active            = src.is_active,
    row_hash             = src.row_hash,
    update_datetime      = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    medication_name, active_ingredient, strength, dosage,
    start_date, end_date, atc_code, reason,
    is_active,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.medication_name, src.active_ingredient, src.strength, src.dosage,
    src.start_date, src.end_date, src.atc_code, src.reason,
    src.is_active,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.sundhed_dk_medications__staging;
