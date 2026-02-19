-- merge_oura_daily_sleep.sql
-- Per-source merge: Oura API -> silver.daily_sleep
-- Business key: day (one row per day)
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
--
-- Usage: python run_merge.py silver/merge_oura_daily_sleep.sql

CREATE OR REPLACE TABLE silver.daily_sleep__staging AS
WITH deduped AS (
    SELECT *,
        make_date(year::INTEGER, month::VARCHAR::INTEGER, day::VARCHAR::INTEGER) AS full_date,
        ROW_NUMBER() OVER (
            PARTITION BY year, month, day
            ORDER BY _ingested_at_1 DESC
        ) AS rn
    FROM bronze.stg_oura_daily_sleep
    WHERE day IS NOT NULL
)
SELECT
    (year::INTEGER * 10000 + month::VARCHAR::INTEGER * 100 + day::VARCHAR::INTEGER)::INTEGER AS sk_date,
    full_date                              AS day,
    score::INTEGER                         AS sleep_score,
    timestamp::TIMESTAMP                   AS timestamp,
    contributors.deep_sleep::INTEGER       AS contributor_deep_sleep,
    contributors.efficiency::INTEGER       AS contributor_efficiency,
    contributors.latency::INTEGER          AS contributor_latency,
    contributors.rem_sleep::INTEGER        AS contributor_rem_sleep,
    contributors.restfulness::INTEGER      AS contributor_restfulness,
    contributors.timing::INTEGER           AS contributor_timing,
    contributors.total_sleep::INTEGER      AS contributor_total_sleep,
    md5(full_date::VARCHAR)                AS business_key_hash,
    md5(
        coalesce(cast(score AS VARCHAR), '')                     || '||' ||
        coalesce(cast(contributors.deep_sleep AS VARCHAR), '')   || '||' ||
        coalesce(cast(contributors.efficiency AS VARCHAR), '')   || '||' ||
        coalesce(cast(contributors.latency AS VARCHAR), '')      || '||' ||
        coalesce(cast(contributors.rem_sleep AS VARCHAR), '')    || '||' ||
        coalesce(cast(contributors.restfulness AS VARCHAR), '')  || '||' ||
        coalesce(cast(contributors.timing AS VARCHAR), '')       || '||' ||
        coalesce(cast(contributors.total_sleep AS VARCHAR), '')
    )                                      AS row_hash,
    current_timestamp                      AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.daily_sleep AS target
USING silver.daily_sleep__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date                    = src.sk_date,
    day                        = src.day,
    sleep_score                = src.sleep_score,
    timestamp                  = src.timestamp,
    contributor_deep_sleep     = src.contributor_deep_sleep,
    contributor_efficiency     = src.contributor_efficiency,
    contributor_latency        = src.contributor_latency,
    contributor_rem_sleep      = src.contributor_rem_sleep,
    contributor_restfulness    = src.contributor_restfulness,
    contributor_timing         = src.contributor_timing,
    contributor_total_sleep    = src.contributor_total_sleep,
    row_hash                   = src.row_hash,
    update_datetime            = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, sleep_score, timestamp,
    contributor_deep_sleep, contributor_efficiency, contributor_latency,
    contributor_rem_sleep, contributor_restfulness, contributor_timing,
    contributor_total_sleep,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.sleep_score, src.timestamp,
    src.contributor_deep_sleep, src.contributor_efficiency, src.contributor_latency,
    src.contributor_rem_sleep, src.contributor_restfulness, src.contributor_timing,
    src.contributor_total_sleep,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.daily_sleep__staging;
