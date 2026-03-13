-- merge_oura_cardiovascular_age.sql
-- Per-source merge: Oura API -> silver.cardiovascular_age
-- Business key: day (one row per day)
-- Note: Bronze table may contain both API rows (year/month/day Hive partitions)
--       and CSV backfill rows. API pattern (make_date) is primary.
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
--
-- Usage: python run_merge.py silver/merge_oura_cardiovascular_age.sql

CREATE OR REPLACE TABLE silver.cardiovascular_age__staging AS
WITH deduped AS (
    SELECT *,
        make_date(year::INTEGER, month::VARCHAR::INTEGER, day::VARCHAR::INTEGER) AS full_date,
        ROW_NUMBER() OVER (
            PARTITION BY year, month, day
            ORDER BY _ingested_at_1 DESC
        ) AS rn
    FROM bronze.stg_oura_daily_cardiovascular_age
    WHERE day IS NOT NULL
)
SELECT
    (year::INTEGER * 10000 + month::VARCHAR::INTEGER * 100 + day::VARCHAR::INTEGER)::INTEGER AS sk_date,
    full_date                              AS day,
    vascular_age::INTEGER                  AS vascular_age,
    md5(full_date::VARCHAR)                AS business_key_hash,
    md5(
        coalesce(cast(vascular_age AS VARCHAR), '')
    )                                      AS row_hash,
    current_timestamp                      AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.cardiovascular_age AS target
USING silver.cardiovascular_age__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date         = src.sk_date,
    day             = src.day,
    vascular_age    = src.vascular_age,
    row_hash        = src.row_hash,
    update_datetime = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, vascular_age,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.vascular_age,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.cardiovascular_age__staging;
