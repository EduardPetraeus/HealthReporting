-- merge_oura_daily_spo2.sql
-- Per-source merge: Oura API -> silver.daily_spo2
-- Business key: day (one row per day)
-- Note: spo2_avg_pct is pre-flattened in bronze.stg_oura_daily_spo2
--
-- Usage: python run_merge.py silver/merge_oura_daily_spo2.sql

CREATE OR REPLACE TABLE silver.daily_spo2__staging AS
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY day ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_oura_daily_spo2
    WHERE day IS NOT NULL
)
SELECT
    (year(day::DATE) * 10000 + month(day::DATE) * 100 + day(day::DATE))::INTEGER AS sk_date,
    day::DATE                             AS day,
    spo2_avg_pct::DOUBLE                  AS spo2_avg_pct,
    breathing_disturbance_index::DOUBLE   AS breathing_disturbance_index,
    md5(coalesce(day, ''))                AS business_key_hash,
    md5(
        coalesce(cast(spo2_avg_pct AS VARCHAR), '')                  || '||' ||
        coalesce(cast(breathing_disturbance_index AS VARCHAR), '')
    )                                     AS row_hash,
    current_timestamp                     AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.daily_spo2 AS target
USING silver.daily_spo2__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date                      = src.sk_date,
    day                          = src.day,
    spo2_avg_pct                 = src.spo2_avg_pct,
    breathing_disturbance_index  = src.breathing_disturbance_index,
    row_hash                     = src.row_hash,
    update_datetime              = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, spo2_avg_pct, breathing_disturbance_index,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.spo2_avg_pct, src.breathing_disturbance_index,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.daily_spo2__staging;
