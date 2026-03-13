-- merge_oura_rest_mode_period.sql
-- Per-source merge: Oura API -> silver.oura_rest_mode_period
-- Business key: id (unique per rest mode period)
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
--
-- Usage: python run_merge.py silver/merge_oura_rest_mode_period.sql

CREATE OR REPLACE TABLE silver.oura_rest_mode_period__staging AS
WITH deduped AS (
    SELECT *,
        make_date(year::INTEGER, month::VARCHAR::INTEGER, day::VARCHAR::INTEGER) AS full_date,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at_1 DESC) AS rn
    FROM bronze.stg_oura_rest_mode_period
    WHERE day IS NOT NULL
)
SELECT
    (year::INTEGER * 10000 + month::VARCHAR::INTEGER * 100 + day::VARCHAR::INTEGER)::INTEGER AS sk_date,
    full_date                              AS day,
    id,
    start_day::DATE                        AS start_day,
    end_day::DATE                          AS end_day,
    start_time::TIMESTAMP                  AS start_time,
    end_time::TIMESTAMP                    AS end_time,
    episodes,
    md5(coalesce(id, ''))                  AS business_key_hash,
    md5(
        coalesce(id, '')                               || '||' ||
        coalesce(cast(start_day AS VARCHAR), '')        || '||' ||
        coalesce(cast(end_day AS VARCHAR), '')          || '||' ||
        coalesce(cast(start_time AS VARCHAR), '')       || '||' ||
        coalesce(cast(end_time AS VARCHAR), '')
    )                                      AS row_hash,
    current_timestamp                      AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.oura_rest_mode_period AS target
USING silver.oura_rest_mode_period__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date         = src.sk_date,
    day             = src.day,
    id              = src.id,
    start_day       = src.start_day,
    end_day         = src.end_day,
    start_time      = src.start_time,
    end_time        = src.end_time,
    episodes        = src.episodes,
    row_hash        = src.row_hash,
    update_datetime = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, id, start_day, end_day, start_time, end_time, episodes,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.id, src.start_day, src.end_day, src.start_time, src.end_time, src.episodes,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.oura_rest_mode_period__staging;
