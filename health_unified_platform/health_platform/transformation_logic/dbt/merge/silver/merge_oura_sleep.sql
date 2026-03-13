-- merge_oura_sleep.sql
-- Per-source merge: Oura (API + CSV) -> silver.oura_sleep
-- Business key: id (each sleep session is unique)
-- Handles both API rows (Hive year/month/day partitions) and CSV rows (day as full date).
-- API is prioritized over CSV when both exist for the same id.
-- Note: This is the detailed sleep session endpoint, not the daily_sleep summary.
--
-- Usage: python run_merge.py silver/merge_oura_sleep.sql

CREATE OR REPLACE TABLE silver.oura_sleep__staging AS
WITH source_data AS (
    SELECT *,
        COALESCE(
            TRY_CAST(make_date(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER)) AS DATE),
            TRY_CAST(day AS DATE)
        ) AS full_date,
        CASE WHEN year IS NOT NULL THEN 1 ELSE 2 END AS source_rank,
        COALESCE(_ingested_at_1, _ingested_at) AS ingested_at
    FROM bronze.stg_oura_sleep
    WHERE id IS NOT NULL
),
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY source_rank ASC, ingested_at DESC) AS rn
    FROM source_data
)
SELECT
    (year(full_date) * 10000 + month(full_date) * 100 + day(full_date))::INTEGER AS sk_date,
    full_date                              AS day,
    id,
    average_breath::DOUBLE                 AS average_breath,
    average_heart_rate::DOUBLE             AS average_heart_rate,
    average_hrv::INTEGER                   AS average_hrv,
    awake_time::INTEGER                    AS awake_time,
    bedtime_end::TIMESTAMP                 AS bedtime_end,
    bedtime_start::TIMESTAMP               AS bedtime_start,
    deep_sleep_duration::INTEGER           AS deep_sleep_duration,
    efficiency::INTEGER                    AS efficiency,
    latency::INTEGER                       AS latency,
    light_sleep_duration::INTEGER          AS light_sleep_duration,
    low_battery_alert::BOOLEAN             AS low_battery_alert,
    lowest_heart_rate::INTEGER             AS lowest_heart_rate,
    period::INTEGER                        AS period,
    readiness_score_delta::DOUBLE          AS readiness_score_delta,
    rem_sleep_duration::INTEGER            AS rem_sleep_duration,
    restless_periods::INTEGER              AS restless_periods,
    sleep_score_delta::DOUBLE              AS sleep_score_delta,
    time_in_bed::INTEGER                   AS time_in_bed,
    total_sleep_duration::INTEGER          AS total_sleep_duration,
    type::VARCHAR                          AS type,
    md5(coalesce(id, ''))                  AS business_key_hash,
    md5(
        coalesce(id, '')                                       || '||' ||
        coalesce(cast(average_breath AS VARCHAR), '')           || '||' ||
        coalesce(cast(average_heart_rate AS VARCHAR), '')       || '||' ||
        coalesce(cast(average_hrv AS VARCHAR), '')              || '||' ||
        coalesce(cast(awake_time AS VARCHAR), '')               || '||' ||
        coalesce(cast(bedtime_end AS VARCHAR), '')              || '||' ||
        coalesce(cast(bedtime_start AS VARCHAR), '')            || '||' ||
        coalesce(cast(deep_sleep_duration AS VARCHAR), '')      || '||' ||
        coalesce(cast(efficiency AS VARCHAR), '')               || '||' ||
        coalesce(cast(latency AS VARCHAR), '')                  || '||' ||
        coalesce(cast(light_sleep_duration AS VARCHAR), '')     || '||' ||
        coalesce(cast(low_battery_alert AS VARCHAR), '')        || '||' ||
        coalesce(cast(lowest_heart_rate AS VARCHAR), '')        || '||' ||
        coalesce(cast(period AS VARCHAR), '')                   || '||' ||
        coalesce(cast(readiness_score_delta AS VARCHAR), '')    || '||' ||
        coalesce(cast(rem_sleep_duration AS VARCHAR), '')       || '||' ||
        coalesce(cast(restless_periods AS VARCHAR), '')         || '||' ||
        coalesce(cast(sleep_score_delta AS VARCHAR), '')        || '||' ||
        coalesce(cast(time_in_bed AS VARCHAR), '')              || '||' ||
        coalesce(cast(total_sleep_duration AS VARCHAR), '')     || '||' ||
        coalesce(type::VARCHAR, '')
    )                                      AS row_hash,
    current_timestamp                      AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.oura_sleep AS target
USING silver.oura_sleep__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date                = src.sk_date,
    day                    = src.day,
    id                     = src.id,
    average_breath         = src.average_breath,
    average_heart_rate     = src.average_heart_rate,
    average_hrv            = src.average_hrv,
    awake_time             = src.awake_time,
    bedtime_end            = src.bedtime_end,
    bedtime_start          = src.bedtime_start,
    deep_sleep_duration    = src.deep_sleep_duration,
    efficiency             = src.efficiency,
    latency                = src.latency,
    light_sleep_duration   = src.light_sleep_duration,
    low_battery_alert      = src.low_battery_alert,
    lowest_heart_rate      = src.lowest_heart_rate,
    period                 = src.period,
    readiness_score_delta  = src.readiness_score_delta,
    rem_sleep_duration     = src.rem_sleep_duration,
    restless_periods       = src.restless_periods,
    sleep_score_delta      = src.sleep_score_delta,
    time_in_bed            = src.time_in_bed,
    total_sleep_duration   = src.total_sleep_duration,
    type                   = src.type,
    row_hash               = src.row_hash,
    update_datetime        = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, id, average_breath, average_heart_rate, average_hrv,
    awake_time, bedtime_end, bedtime_start, deep_sleep_duration,
    efficiency, latency, light_sleep_duration, low_battery_alert,
    lowest_heart_rate, period, readiness_score_delta, rem_sleep_duration,
    restless_periods, sleep_score_delta, time_in_bed, total_sleep_duration, type,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.id, src.average_breath, src.average_heart_rate, src.average_hrv,
    src.awake_time, src.bedtime_end, src.bedtime_start, src.deep_sleep_duration,
    src.efficiency, src.latency, src.light_sleep_duration, src.low_battery_alert,
    src.lowest_heart_rate, src.period, src.readiness_score_delta, src.rem_sleep_duration,
    src.restless_periods, src.sleep_score_delta, src.time_in_bed, src.total_sleep_duration, src.type,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.oura_sleep__staging;
