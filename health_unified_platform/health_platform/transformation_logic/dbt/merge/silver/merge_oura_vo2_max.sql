-- merge_oura_vo2_max.sql
-- Per-source merge: Oura -> silver.oura_vo2_max
-- Business key: day (one row per day)
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
--
-- Usage: python run_merge.py silver/merge_oura_vo2_max.sql

CREATE OR REPLACE TABLE silver.oura_vo2_max__staging AS
WITH deduped AS (
    SELECT *,
        make_date(year::INTEGER, month::VARCHAR::INTEGER, day::VARCHAR::INTEGER) AS full_date,
        ROW_NUMBER() OVER (
            PARTITION BY year, month, day
            ORDER BY _ingested_at_1 DESC
        ) AS rn
    FROM bronze.stg_oura_vo2_max
    WHERE day IS NOT NULL
)
SELECT
    (year::INTEGER * 10000 + month::VARCHAR::INTEGER * 100 + day::VARCHAR::INTEGER)::INTEGER AS sk_date,
    full_date                              AS day,
    id,
    vo2_max::DOUBLE                        AS vo2_max,
    md5(full_date::VARCHAR)                AS business_key_hash,
    md5(
        coalesce(id, '')                           || '||' ||
        coalesce(cast(vo2_max AS VARCHAR), '')
    )                                      AS row_hash,
    current_timestamp                      AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.oura_vo2_max AS target
USING silver.oura_vo2_max__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date         = src.sk_date,
    day             = src.day,
    id              = src.id,
    vo2_max         = src.vo2_max,
    row_hash        = src.row_hash,
    update_datetime = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, id, vo2_max,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.id, src.vo2_max,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.oura_vo2_max__staging;
