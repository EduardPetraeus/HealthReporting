-- merge_oura_ring_configuration.sql
-- Per-source merge: Oura API -> silver.oura_ring_configuration
-- Business key: id (unique per ring configuration)
-- Note: day col = hive partition day-of-month. Full date reconstructed from year/month/day.
--
-- Usage: python run_merge.py silver/merge_oura_ring_configuration.sql

CREATE OR REPLACE TABLE silver.oura_ring_configuration__staging AS
WITH deduped AS (
    SELECT *,
        make_date(year::INTEGER, month::VARCHAR::INTEGER, day::VARCHAR::INTEGER) AS full_date,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY _ingested_at_1 DESC) AS rn
    FROM bronze.stg_oura_ring_configuration
    WHERE day IS NOT NULL
)
SELECT
    (year::INTEGER * 10000 + month::VARCHAR::INTEGER * 100 + day::VARCHAR::INTEGER)::INTEGER AS sk_date,
    full_date                              AS day,
    id,
    color::VARCHAR                         AS color,
    design::VARCHAR                        AS design,
    firmware_version::VARCHAR              AS firmware_version,
    hardware_type::VARCHAR                 AS hardware_type,
    set_up_at::TIMESTAMP                   AS set_up_at,
    size::INTEGER                          AS size,
    md5(coalesce(id, ''))                  AS business_key_hash,
    md5(
        coalesce(id, '')                                   || '||' ||
        coalesce(color::VARCHAR, '')                        || '||' ||
        coalesce(design::VARCHAR, '')                       || '||' ||
        coalesce(firmware_version::VARCHAR, '')              || '||' ||
        coalesce(hardware_type::VARCHAR, '')                 || '||' ||
        coalesce(cast(set_up_at AS VARCHAR), '')             || '||' ||
        coalesce(cast(size AS VARCHAR), '')
    )                                      AS row_hash,
    current_timestamp                      AS load_datetime
FROM deduped WHERE rn = 1;

MERGE INTO silver.oura_ring_configuration AS target
USING silver.oura_ring_configuration__staging AS src
ON target.business_key_hash = src.business_key_hash

WHEN MATCHED AND target.row_hash <> src.row_hash THEN UPDATE SET
    sk_date            = src.sk_date,
    day                = src.day,
    id                 = src.id,
    color              = src.color,
    design             = src.design,
    firmware_version   = src.firmware_version,
    hardware_type      = src.hardware_type,
    set_up_at          = src.set_up_at,
    size               = src.size,
    row_hash           = src.row_hash,
    update_datetime    = current_timestamp

WHEN NOT MATCHED THEN INSERT (
    sk_date, day, id, color, design, firmware_version, hardware_type, set_up_at, size,
    business_key_hash, row_hash, load_datetime, update_datetime
) VALUES (
    src.sk_date, src.day, src.id, src.color, src.design, src.firmware_version,
    src.hardware_type, src.set_up_at, src.size,
    src.business_key_hash, src.row_hash, current_timestamp, current_timestamp
);

DROP TABLE IF EXISTS silver.oura_ring_configuration__staging;
