-- Per-source merge: sundhed.dk lab results -> silver.sundhed_dk_lab_results
-- Business key: test_date + marker_name (one row per marker per test date)
-- Source: bronze.stg_sundhed_dk_lab_results (parquet files from sundhed.dk HTML parser)
--
-- Usage: python run_merge.py silver/merge_sundhed_dk_lab_results.sql

CREATE OR REPLACE TABLE silver.sundhed_dk_lab_results__staging AS
WITH deduped AS (
    SELECT *,
        md5(
            coalesce(cast(test_date AS VARCHAR), '') || '||' ||
            coalesce(marker_name, '')
        )                                       AS business_key_hash,
        ROW_NUMBER() OVER (
            PARTITION BY md5(
                coalesce(cast(test_date AS VARCHAR), '') || '||' ||
                coalesce(marker_name, '')
            )
            ORDER BY load_datetime DESC
        ) AS rn
    FROM bronze.stg_sundhed_dk_lab_results
    WHERE marker_name IS NOT NULL
      AND test_date IS NOT NULL
)
SELECT
    CAST(test_date AS DATE)                    AS test_date,
    marker_name,
    CAST(value_numeric AS DOUBLE)              AS value_numeric,
    value_text,
    unit,
    CAST(reference_min AS DOUBLE)              AS reference_min,
    CAST(reference_max AS DOUBLE)              AS reference_max,
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
    )                                          AS row_hash,
    current_timestamp                          AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.sundhed_dk_lab_results AS target
USING silver.sundhed_dk_lab_results__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    test_date            = src.test_date,
    marker_name          = src.marker_name,
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
    test_date, marker_name,
    value_numeric, value_text, unit,
    reference_min, reference_max, reference_direction,
    status,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.test_date, src.marker_name,
    src.value_numeric, src.value_text, src.unit,
    src.reference_min, src.reference_max, src.reference_direction,
    src.status,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.sundhed_dk_lab_results__staging;
