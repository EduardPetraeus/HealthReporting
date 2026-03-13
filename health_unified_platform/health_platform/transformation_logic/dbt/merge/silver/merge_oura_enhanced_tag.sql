-- merge_oura_enhanced_tag.sql
-- Per-source merge: Oura API -> silver.enhanced_tag
-- Business key: id (unique per enhanced tag)
-- Note: Bronze table may contain both API rows (year/month/day Hive partitions)
--       and CSV backfill rows. API pattern (make_date) is primary.
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
--
-- Usage: python run_merge.py silver/merge_oura_enhanced_tag.sql

CREATE OR REPLACE TABLE silver.enhanced_tag__staging AS
WITH deduped AS (
    SELECT *,
        make_date(year::INTEGER, month::VARCHAR::INTEGER, day::VARCHAR::INTEGER) AS full_date,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at_1 DESC) AS rn
    FROM bronze.stg_oura_enhanced_tag
    WHERE day IS NOT NULL
)
SELECT
    (year::INTEGER * 10000 + month::VARCHAR::INTEGER * 100 + day::VARCHAR::INTEGER)::INTEGER AS sk_date,
    full_date                              AS day,
    id,
    start_day::DATE                        AS start_day,
    start_time::VARCHAR                    AS start_time,
    end_day::DATE                          AS end_day,
    end_time::VARCHAR                      AS end_time,
    tag_type_code::VARCHAR                 AS tag_type_code,
    custom_tag_name::VARCHAR               AS custom_tag_name,
    comment::VARCHAR                       AS comment,
    md5(coalesce(id, ''))                  AS business_key_hash,
    md5(
        coalesce(id, '')                               || '||' ||
        coalesce(cast(start_day AS VARCHAR), '')        || '||' ||
        coalesce(start_time::VARCHAR, '')               || '||' ||
        coalesce(cast(end_day AS VARCHAR), '')          || '||' ||
        coalesce(end_time::VARCHAR, '')                 || '||' ||
        coalesce(tag_type_code::VARCHAR, '')             || '||' ||
        coalesce(custom_tag_name::VARCHAR, '')           || '||' ||
        coalesce(comment::VARCHAR, '')
    )                                      AS row_hash,
    current_timestamp                      AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.enhanced_tag AS target
USING silver.enhanced_tag__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date          = src.sk_date,
    day              = src.day,
    id               = src.id,
    start_day        = src.start_day,
    start_time       = src.start_time,
    end_day          = src.end_day,
    end_time         = src.end_time,
    tag_type_code    = src.tag_type_code,
    custom_tag_name  = src.custom_tag_name,
    comment          = src.comment,
    row_hash         = src.row_hash,
    update_datetime  = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, id, start_day, start_time, end_day, end_time,
    tag_type_code, custom_tag_name, comment,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.id, src.start_day, src.start_time, src.end_day, src.end_time,
    src.tag_type_code, src.custom_tag_name, src.comment,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.enhanced_tag__staging;
