-- merge_weather_daily.sql
-- Per-source merge: Open-Meteo API -> silver.daily_weather
-- Business key: day (one row per day)
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
--
-- Usage: python run_merge.py silver/merge_weather_daily.sql

CREATE OR REPLACE TABLE silver.daily_weather__staging AS
WITH deduped AS (
    SELECT *,
        make_date(year::INTEGER, month::VARCHAR::INTEGER, day::VARCHAR::INTEGER) AS full_date,
        ROW_NUMBER() OVER (
            PARTITION BY year, month, day
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_weather_open_meteo
    WHERE day IS NOT NULL
)
SELECT
    (year::INTEGER * 10000 + month::VARCHAR::INTEGER * 100 + day::VARCHAR::INTEGER)::INTEGER AS sk_date,
    full_date                                   AS day,
    temp_max_c::DOUBLE                          AS temp_max_c,
    temp_min_c::DOUBLE                          AS temp_min_c,
    precipitation_mm::DOUBLE                    AS precipitation_mm,
    wind_speed_max_kmh::DOUBLE                  AS wind_speed_max_kmh,
    uv_index_max::DOUBLE                        AS uv_index_max,
    md5(full_date::VARCHAR)                     AS business_key_hash,
    md5(
        coalesce(cast(temp_max_c AS VARCHAR), '')           || '||' ||
        coalesce(cast(temp_min_c AS VARCHAR), '')           || '||' ||
        coalesce(cast(precipitation_mm AS VARCHAR), '')     || '||' ||
        coalesce(cast(wind_speed_max_kmh AS VARCHAR), '')   || '||' ||
        coalesce(cast(uv_index_max AS VARCHAR), '')
    )                                           AS row_hash,
    current_timestamp                           AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.daily_weather AS target
USING silver.daily_weather__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date                = src.sk_date,
    day                    = src.day,
    temp_max_c             = src.temp_max_c,
    temp_min_c             = src.temp_min_c,
    precipitation_mm       = src.precipitation_mm,
    wind_speed_max_kmh     = src.wind_speed_max_kmh,
    uv_index_max           = src.uv_index_max,
    row_hash               = src.row_hash,
    update_datetime        = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, temp_max_c, temp_min_c, precipitation_mm,
    wind_speed_max_kmh, uv_index_max,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.temp_max_c, src.temp_min_c, src.precipitation_mm,
    src.wind_speed_max_kmh, src.uv_index_max,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.daily_weather__staging;
