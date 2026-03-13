-- merge_oura_enhanced_tag.sql
-- Per-source merge: Oura (API + CSV) -> silver.enhanced_tag
-- Business key: id (unique per enhanced tag)
-- Handles both API rows (Hive year/month/day partitions) and CSV rows (day as full date).
-- API is prioritized over CSV when both exist for the same id.
-- Note: API uses 'custom_name', CSV uses 'custom_tag_name' — COALESCE handles both.
--
-- Usage: python run_merge.py silver/merge_oura_enhanced_tag.sql

CREATE OR REPLACE TABLE silver.enhanced_tag__staging AS
WITH source_data AS (
    SELECT *,
        -- Resolve date from either API (year/month/day partitions) or CSV (start_day as date)
        COALESCE(
            TRY_CAST(make_date(TRY_CAST(year AS INTEGER), TRY_CAST(month AS INTEGER), TRY_CAST(day AS INTEGER)) AS DATE),
            TRY_CAST(start_day AS DATE)
        ) AS full_date,
        -- Resolve custom_tag_name from API (custom_name) or CSV (custom_tag_name)
        COALESCE(custom_name, custom_tag_name) AS resolved_custom_tag_name,
        -- API rows get priority (source_rank=1) over CSV (source_rank=2)
        CASE WHEN year IS NOT NULL THEN 1 ELSE 2 END AS source_rank,
        COALESCE(_ingested_at_1, _ingested_at) AS ingested_at
    FROM bronze.stg_oura_enhanced_tag
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
    start_day::DATE                        AS start_day,
    start_time::VARCHAR                    AS start_time,
    end_day::DATE                          AS end_day,
    end_time::VARCHAR                      AS end_time,
    tag_type_code::VARCHAR                 AS tag_type_code,
    resolved_custom_tag_name::VARCHAR      AS custom_tag_name,
    comment::VARCHAR                       AS comment,
    md5(coalesce(id, ''))                  AS business_key_hash,
    md5(
        coalesce(id, '')                               || '||' ||
        coalesce(cast(start_day AS VARCHAR), '')        || '||' ||
        coalesce(start_time::VARCHAR, '')               || '||' ||
        coalesce(cast(end_day AS VARCHAR), '')          || '||' ||
        coalesce(end_time::VARCHAR, '')                 || '||' ||
        coalesce(tag_type_code::VARCHAR, '')             || '||' ||
        coalesce(resolved_custom_tag_name::VARCHAR, '') || '||' ||
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
