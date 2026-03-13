-- merge_oura_daily_stress.sql
-- Per-source merge: Oura (API + CSV) -> silver.daily_stress
-- Business key: day (one row per day)
-- Handles both API rows (Hive year/month/day partitions) and CSV rows (day as full date).
-- API is prioritized over CSV when both exist for the same day.
-- Note: stress_high and recovery_high are in seconds.
--
-- Usage: python run_merge.py silver/merge_oura_daily_stress.sql

CREATE OR REPLACE TABLE silver.daily_stress__staging AS
WITH source_data AS (
    SELECT *,
        COALESCE(
            TRY_CAST(make_date(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER)) AS DATE),
            TRY_CAST(day AS DATE)
        ) AS full_date,
        CASE WHEN year IS NOT NULL THEN 1 ELSE 2 END AS source_rank,
        COALESCE(_ingested_at_1::TIMESTAMP, _ingested_at::TIMESTAMP) AS ingested_at
    FROM bronze.stg_oura_daily_stress
    WHERE day IS NOT NULL
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY full_date
            ORDER BY source_rank ASC, ingested_at DESC
        ) AS rn
    FROM source_data
    WHERE full_date IS NOT NULL
)
SELECT
    (year(full_date) * 10000 + month(full_date) * 100 + day(full_date))::INTEGER AS sk_date,
    full_date                             AS day,
    day_summary,
    stress_high::INTEGER                  AS stress_high,
    recovery_high::INTEGER                AS recovery_high,
    md5(full_date::VARCHAR)               AS business_key_hash,
    md5(
        coalesce(day_summary, '')                          || '||' ||
        coalesce(cast(stress_high AS VARCHAR), '')         || '||' ||
        coalesce(cast(recovery_high AS VARCHAR), '')
    )                                     AS row_hash,
    current_timestamp                     AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.daily_stress AS target
USING silver.daily_stress__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date         = src.sk_date,
    day             = src.day,
    day_summary     = src.day_summary,
    stress_high     = src.stress_high,
    recovery_high   = src.recovery_high,
    row_hash        = src.row_hash,
    update_datetime = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, day_summary, stress_high, recovery_high,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.day_summary, src.stress_high, src.recovery_high,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.daily_stress__staging;
