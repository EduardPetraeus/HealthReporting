-- merge_oura_daily_stress.sql
-- Per-source merge: Oura API -> silver.daily_stress
-- Business key: day (one row per day)
-- Note: stress_high and recovery_high are in seconds.
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
--
-- Usage: python run_merge.py silver/merge_oura_daily_stress.sql

CREATE OR REPLACE TABLE silver.daily_stress__staging AS
WITH deduped AS (
    SELECT *,
        make_date(year::INTEGER, month::VARCHAR::INTEGER, day::VARCHAR::INTEGER) AS full_date,
        ROW_NUMBER() OVER (
            PARTITION BY year, month, day
            ORDER BY _ingested_at_1 DESC
        ) AS rn
    FROM bronze.stg_oura_daily_stress
    WHERE day IS NOT NULL
)
SELECT
    (year::INTEGER * 10000 + month::VARCHAR::INTEGER * 100 + day::VARCHAR::INTEGER)::INTEGER AS sk_date,
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
