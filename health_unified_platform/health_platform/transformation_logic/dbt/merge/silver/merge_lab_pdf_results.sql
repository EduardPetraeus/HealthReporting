-- Per-source merge: Lab PDF -> silver.lab_results
-- Business key: test_id + marker_name (one row per marker per test)
-- Source: bronze.stg_lab_pdf_results (parquet files from lab PDF parser)
--
-- Usage: python run_merge.py silver/merge_lab_pdf_results.sql

CREATE OR REPLACE TABLE silver.lab_results__staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY business_key_hash
            ORDER BY load_datetime DESC
        ) AS rn
    FROM bronze.stg_lab_pdf_results
    WHERE marker_name IS NOT NULL
      AND test_id IS NOT NULL
)
SELECT
    test_id,
    CAST(test_date AS DATE)             AS test_date,
    test_type,
    test_name,
    lab_name,
    lab_accreditation,
    marker_name,
    marker_category,
    CAST(value_numeric AS DOUBLE)       AS value_numeric,
    value_text,
    unit,
    CAST(reference_min AS DOUBLE)       AS reference_min,
    CAST(reference_max AS DOUBLE)       AS reference_max,
    reference_direction,
    status,
    business_key_hash,
    md5(
        coalesce(cast(value_numeric AS VARCHAR), '') || '||' ||
        coalesce(value_text, '')                     || '||' ||
        coalesce(unit, '')                           || '||' ||
        coalesce(cast(reference_min AS VARCHAR), '') || '||' ||
        coalesce(cast(reference_max AS VARCHAR), '') || '||' ||
        coalesce(status, '')
    )                                   AS row_hash,
    current_timestamp                   AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.lab_results AS target
USING silver.lab_results__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    test_id              = src.test_id,
    test_date            = src.test_date,
    test_type            = src.test_type,
    test_name            = src.test_name,
    lab_name             = src.lab_name,
    lab_accreditation    = src.lab_accreditation,
    marker_name          = src.marker_name,
    marker_category      = src.marker_category,
    value_numeric        = src.value_numeric,
    value_text           = src.value_text,
    unit                 = src.unit,
    reference_min        = src.reference_min,
    reference_max        = src.reference_max,
    reference_direction  = src.reference_direction,
    status               = src.status,
    row_hash             = src.row_hash,
    update_datetime      = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    test_id, test_date, test_type, test_name,
    lab_name, lab_accreditation,
    marker_name, marker_category,
    value_numeric, value_text, unit,
    reference_min, reference_max, reference_direction,
    status,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.test_id, src.test_date, src.test_type, src.test_name,
    src.lab_name, src.lab_accreditation,
    src.marker_name, src.marker_category,
    src.value_numeric, src.value_text, src.unit,
    src.reference_min, src.reference_max, src.reference_direction,
    src.status,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.lab_results__staging;
