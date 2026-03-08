-- merge_oura_csv_enhanced_tag.sql
-- Per-source merge: Oura CSV -> silver.enhanced_tag
-- Follows the staging + MERGE + drop pattern, adapted for DuckDB.
--
-- Usage: python run_merge.py silver/merge_oura_csv_enhanced_tag.sql

-- Step 1: Create staging table with deduplication
CREATE OR REPLACE TABLE silver.enhanced_tag__staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY start_day, tag_type_code
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM bronze.stg_oura_csv_enhancedtag
    WHERE start_day IS NOT NULL
)
SELECT
    (year(start_day::DATE) * 10000 + month(start_day::DATE) * 100 + day(start_day::DATE))::INTEGER AS sk_date,
    start_day::DATE AS start_day,
    start_time,
    TRY_CAST(end_day AS DATE) AS end_day,
    end_time,
    tag_type_code,
    custom_tag_name,
    comment,
    'oura' AS source_name,
    md5(
        coalesce(cast(start_day AS VARCHAR), '') || '||' ||
        coalesce(tag_type_code, '') || '||' || 'oura'
    ) AS business_key_hash,
    md5(
        coalesce(cast(start_day AS VARCHAR), '') || '||' ||
        coalesce(start_time, '') || '||' ||
        coalesce(cast(end_day AS VARCHAR), '') || '||' ||
        coalesce(end_time, '') || '||' ||
        coalesce(tag_type_code, '') || '||' ||
        coalesce(custom_tag_name, '') || '||' ||
        coalesce(comment, '')
    ) AS row_hash,
    current_timestamp AS load_datetime
FROM deduped
WHERE rn = 1;

-- Step 2: Merge staging into silver.enhanced_tag
MERGE INTO silver.enhanced_tag AS target
USING silver.enhanced_tag__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    sk_date           = src.sk_date,
    start_day         = src.start_day,
    start_time        = src.start_time,
    end_day           = src.end_day,
    end_time          = src.end_time,
    tag_type_code     = src.tag_type_code,
    custom_tag_name   = src.custom_tag_name,
    comment           = src.comment,
    source_name       = src.source_name,
    row_hash          = src.row_hash,
    update_datetime   = current_timestamp

WHEN NOT MATCHED THEN
  INSERT (
    sk_date, start_day, start_time, end_day, end_time, tag_type_code, custom_tag_name, comment,
    source_name, business_key_hash, row_hash, load_datetime, update_datetime
  )
  VALUES (
    src.sk_date, src.start_day, src.start_time, src.end_day, src.end_time, src.tag_type_code, src.custom_tag_name, src.comment,
    src.source_name, src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
  );

-- Step 3: Drop staging table
DROP TABLE IF EXISTS silver.enhanced_tag__staging;
