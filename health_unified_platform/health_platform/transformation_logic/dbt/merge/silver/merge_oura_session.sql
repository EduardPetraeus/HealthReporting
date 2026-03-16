-- merge_oura_session.sql
-- Per-source merge: Oura API -> silver.oura_session
-- Business key: id (unique per session entry)
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
--
-- Usage: python run_merge.py silver/merge_oura_session.sql

CREATE OR REPLACE TABLE silver.oura_session__staging AS
WITH deduped AS (
    SELECT *,
        COALESCE(
            TRY_CAST(make_date(year::INTEGER, TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER)) AS DATE),
            TRY_CAST(day AS DATE)
        ) AS full_date,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at_1 DESC) AS rn
    FROM bronze.stg_oura_session
    WHERE day IS NOT NULL AND id IS NOT NULL
)
SELECT
    (year::INTEGER * 10000 + TRY_CAST(month AS INTEGER) * 100 + TRY_CAST(day AS INTEGER))::INTEGER AS sk_date,
    full_date                              AS day,
    id,
    TRY_CAST(start_datetime AS TIMESTAMP)  AS start_datetime,
    TRY_CAST(end_datetime AS TIMESTAMP)    AS end_datetime,
    type::VARCHAR                          AS type,
    mood::VARCHAR                          AS mood,
    heart_rate::VARCHAR                    AS heart_rate,
    heart_rate_variability::VARCHAR        AS heart_rate_variability,
    motion_count::VARCHAR                  AS motion_count,
    md5(coalesce(id, ''))                  AS business_key_hash,
    md5(
        coalesce(id, '')                                        || '||' ||
        coalesce(cast(TRY_CAST(start_datetime AS TIMESTAMP) AS VARCHAR), '') || '||' ||
        coalesce(cast(TRY_CAST(end_datetime AS TIMESTAMP) AS VARCHAR), '')   || '||' ||
        coalesce(type::VARCHAR, '')                             || '||' ||
        coalesce(mood::VARCHAR, '')                             || '||' ||
        coalesce(heart_rate::VARCHAR, '')                       || '||' ||
        coalesce(heart_rate_variability::VARCHAR, '')            || '||' ||
        coalesce(motion_count::VARCHAR, '')
    )                                      AS row_hash,
    current_timestamp                      AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.oura_session AS target
USING silver.oura_session__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date                 = src.sk_date,
    day                     = src.day,
    id                      = src.id,
    start_datetime          = src.start_datetime,
    end_datetime            = src.end_datetime,
    type                    = src.type,
    mood                    = src.mood,
    heart_rate              = src.heart_rate,
    heart_rate_variability  = src.heart_rate_variability,
    motion_count            = src.motion_count,
    row_hash                = src.row_hash,
    update_datetime         = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, id, start_datetime, end_datetime, type, mood,
    heart_rate, heart_rate_variability, motion_count,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.id, src.start_datetime, src.end_datetime, src.type, src.mood,
    src.heart_rate, src.heart_rate_variability, src.motion_count,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.oura_session__staging;
