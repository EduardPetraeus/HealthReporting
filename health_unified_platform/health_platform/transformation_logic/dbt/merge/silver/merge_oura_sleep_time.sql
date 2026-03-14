-- merge_oura_sleep_time.sql
-- Per-source merge: Oura API -> silver.sleep_time
-- Business key: id (unique per sleep_time recommendation)
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
-- Columns: id, day, optimal_bedtime (struct), recommendation, status
--
-- Usage: python run_merge.py silver/merge_oura_sleep_time.sql

-- Step 0: Cold-start — create target table if it does not exist yet
CREATE TABLE IF NOT EXISTS silver.sleep_time (
    sk_date                      INTEGER,
    day                          DATE,
    id                           VARCHAR,
    optimal_bedtime_day_tz       INTEGER,
    optimal_bedtime_end_offset   INTEGER,
    optimal_bedtime_start_offset INTEGER,
    recommendation               VARCHAR,
    status                       VARCHAR,
    business_key_hash            VARCHAR,
    row_hash                     VARCHAR,
    load_datetime                TIMESTAMP,
    update_datetime              TIMESTAMP
);

CREATE OR REPLACE TABLE silver.sleep_time__staging AS
WITH deduped AS (
    SELECT *,
        TRY_CAST(make_date(
            TRY_CAST(year AS INTEGER),
            TRY_CAST(month AS INTEGER),
            TRY_CAST(day AS INTEGER)
        ) AS DATE) AS full_date,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at DESC) AS rn
    FROM bronze.stg_oura_sleep_time
    WHERE day IS NOT NULL
)
SELECT
    (TRY_CAST(year AS INTEGER) * 10000 + TRY_CAST(month AS INTEGER) * 100 + TRY_CAST(day AS INTEGER))::INTEGER AS sk_date,
    full_date                              AS day,
    id,
    optimal_bedtime.day_tz::INTEGER        AS optimal_bedtime_day_tz,
    optimal_bedtime.end_offset::INTEGER    AS optimal_bedtime_end_offset,
    optimal_bedtime.start_offset::INTEGER  AS optimal_bedtime_start_offset,
    recommendation::VARCHAR                AS recommendation,
    status::VARCHAR                        AS status,
    md5(coalesce(id, ''))                  AS business_key_hash,
    md5(
        coalesce(id, '')                                                   || '||' ||
        coalesce(cast(optimal_bedtime.day_tz AS VARCHAR), '')              || '||' ||
        coalesce(cast(optimal_bedtime.end_offset AS VARCHAR), '')          || '||' ||
        coalesce(cast(optimal_bedtime.start_offset AS VARCHAR), '')        || '||' ||
        coalesce(recommendation::VARCHAR, '')                              || '||' ||
        coalesce(status::VARCHAR, '')
    )                                      AS row_hash,
    current_timestamp                      AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.sleep_time AS target
USING silver.sleep_time__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date                       = src.sk_date,
    day                           = src.day,
    id                            = src.id,
    optimal_bedtime_day_tz        = src.optimal_bedtime_day_tz,
    optimal_bedtime_end_offset    = src.optimal_bedtime_end_offset,
    optimal_bedtime_start_offset  = src.optimal_bedtime_start_offset,
    recommendation                = src.recommendation,
    status                        = src.status,
    row_hash                      = src.row_hash,
    update_datetime               = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, id,
    optimal_bedtime_day_tz, optimal_bedtime_end_offset, optimal_bedtime_start_offset,
    recommendation, status,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.id,
    src.optimal_bedtime_day_tz, src.optimal_bedtime_end_offset, src.optimal_bedtime_start_offset,
    src.recommendation, src.status,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.sleep_time__staging;
