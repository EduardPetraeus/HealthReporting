-- Per-source merge: sundhed.dk appointments -> silver.sundhed_dk_appointments
-- Business key: referral_date + receiving_clinic + specialty (one row per referral)
-- Source: bronze.stg_sundhed_dk_appointments (parquet files from sundhed.dk HTML parser)
--
-- Usage: python run_merge.py silver/merge_sundhed_dk_appointments.sql

CREATE OR REPLACE TABLE silver.sundhed_dk_appointments__staging AS
WITH deduped AS (
    SELECT *,
        md5(
            coalesce(cast(referral_date AS VARCHAR), '') || '||' ||
            coalesce(receiving_clinic, '')               || '||' ||
            coalesce(specialty, '')                      || '||' ||
            coalesce(section, '')
        )                                       AS business_key_hash,
        ROW_NUMBER() OVER (
            PARTITION BY md5(
                coalesce(cast(referral_date AS VARCHAR), '') || '||' ||
                coalesce(receiving_clinic, '')               || '||' ||
                coalesce(specialty, '')                      || '||' ||
                coalesce(section, '')
            )
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_sundhed_dk_appointments
    WHERE referral_date IS NOT NULL
)
SELECT
    CAST(referral_date AS DATE)                AS referral_date,
    CAST(expiry_date AS DATE)                  AS expiry_date,
    referring_clinic,
    receiving_clinic,
    specialty,
    section,
    CASE
        WHEN expiry_date IS NULL OR CAST(expiry_date AS DATE) >= CURRENT_DATE THEN TRUE
        ELSE FALSE
    END                                        AS is_active,
    DATEDIFF('day', CURRENT_DATE, CAST(expiry_date AS DATE)) AS days_until_expiry,
    business_key_hash,
    md5(
        coalesce(cast(expiry_date AS VARCHAR), '')   || '||' ||
        coalesce(referring_clinic, '')                || '||' ||
        coalesce(section, '')
    )                                          AS row_hash,
    current_timestamp                          AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.sundhed_dk_appointments AS target
USING silver.sundhed_dk_appointments__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    referral_date        = src.referral_date,
    expiry_date          = src.expiry_date,
    referring_clinic     = src.referring_clinic,
    receiving_clinic     = src.receiving_clinic,
    specialty            = src.specialty,
    section              = src.section,
    is_active            = src.is_active,
    days_until_expiry    = src.days_until_expiry,
    row_hash             = src.row_hash,
    update_datetime      = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    referral_date, expiry_date,
    referring_clinic, receiving_clinic, specialty, section,
    is_active, days_until_expiry,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.referral_date, src.expiry_date,
    src.referring_clinic, src.receiving_clinic, src.specialty, src.section,
    src.is_active, src.days_until_expiry,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.sundhed_dk_appointments__staging;
