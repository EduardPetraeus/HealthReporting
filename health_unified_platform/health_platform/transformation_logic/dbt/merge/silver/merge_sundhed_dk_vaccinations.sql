-- Per-source merge: sundhed.dk vaccinations -> silver.sundhed_dk_vaccinations
-- Business key: vaccine_name + vaccine_date (one row per vaccine per date)
-- Source: bronze.stg_sundhed_dk_vaccinations (parquet files from sundhed.dk HTML parser)
--
-- Usage: python run_merge.py silver/merge_sundhed_dk_vaccinations.sql

CREATE OR REPLACE TABLE silver.sundhed_dk_vaccinations__staging AS
WITH deduped AS (
    SELECT *,
        md5(
            coalesce(vaccine_name, '') || '||' ||
            coalesce(cast(vaccine_date AS VARCHAR), '')
        )                                       AS business_key_hash,
        ROW_NUMBER() OVER (
            PARTITION BY md5(
                coalesce(vaccine_name, '') || '||' ||
                coalesce(cast(vaccine_date AS VARCHAR), '')
            )
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_sundhed_dk_vaccinations
    WHERE vaccine_name IS NOT NULL
)
SELECT
    vaccine_name,
    CAST(vaccine_date AS DATE)                 AS vaccine_date,
    given_at,
    duration,
    batch_number,
    CASE
        WHEN vaccine_name LIKE '%mod %'
            THEN TRIM(SUBSTRING(vaccine_name FROM POSITION('mod ' IN vaccine_name) + 4))
        ELSE NULL
    END                                        AS disease_target,
    business_key_hash,
    md5(
        coalesce(given_at, '')                       || '||' ||
        coalesce(duration, '')                       || '||' ||
        coalesce(batch_number, '')
    )                                          AS row_hash,
    current_timestamp                          AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.sundhed_dk_vaccinations AS target
USING silver.sundhed_dk_vaccinations__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    vaccine_name         = src.vaccine_name,
    vaccine_date         = src.vaccine_date,
    given_at             = src.given_at,
    duration             = src.duration,
    batch_number         = src.batch_number,
    disease_target       = src.disease_target,
    row_hash             = src.row_hash,
    update_datetime      = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    vaccine_name, vaccine_date,
    given_at, duration, batch_number,
    disease_target,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.vaccine_name, src.vaccine_date,
    src.given_at, src.duration, src.batch_number,
    src.disease_target,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.sundhed_dk_vaccinations__staging;
