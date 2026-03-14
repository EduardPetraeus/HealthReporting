-- Per-source merge: sundhed.dk e-journal -> silver.sundhed_dk_ejournal
-- Business key: note_date + hospital + department + first 200 chars of note_text
-- Source: bronze.stg_sundhed_dk_ejournal (parquet files from sundhed.dk HTML parser)
--
-- Usage: python run_merge.py silver/merge_sundhed_dk_ejournal.sql

CREATE OR REPLACE TABLE silver.sundhed_dk_ejournal__staging AS
WITH deduped AS (
    SELECT *,
        md5(
            coalesce(cast(note_date AS VARCHAR), '') || '||' ||
            coalesce(hospital, '')                   || '||' ||
            coalesce(department, '')                 || '||' ||
            coalesce(left(note_text, 200), '')
        )                                       AS business_key_hash,
        ROW_NUMBER() OVER (
            PARTITION BY md5(
                coalesce(cast(note_date AS VARCHAR), '') || '||' ||
                coalesce(hospital, '')                   || '||' ||
                coalesce(department, '')                 || '||' ||
                coalesce(left(note_text, 200), '')
            )
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_sundhed_dk_ejournal
    WHERE note_text IS NOT NULL AND note_text != ''
)
SELECT
    CAST(note_date AS DATE)                    AS note_date,
    department,
    hospital,
    note_type,
    note_text,
    business_key_hash,
    md5(
        coalesce(note_type, '')                      || '||' ||
        coalesce(note_text, '')
    )                                          AS row_hash,
    current_timestamp                          AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.sundhed_dk_ejournal AS target
USING silver.sundhed_dk_ejournal__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    note_date            = src.note_date,
    department           = src.department,
    hospital             = src.hospital,
    note_type            = src.note_type,
    note_text            = src.note_text,
    row_hash             = src.row_hash,
    update_datetime      = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    note_date, department, hospital,
    note_type, note_text,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.note_date, src.department, src.hospital,
    src.note_type, src.note_text,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.sundhed_dk_ejournal__staging;
