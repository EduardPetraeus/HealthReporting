-- =============================================================================
-- water_intake.sql
-- Silver: Apple Health water intake tracking
--
-- Source: health_dw.bronze.stg_apple_health_water
--
-- Business key: business_key_hash (timestamp + source_name)
-- Change detection: row_hash over water_ml
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.water_intake (
    sk_date           INTEGER   NOT NULL,
    sk_time           STRING    NOT NULL,
    timestamp         TIMESTAMP,
    water_ml          DOUBLE,
    source_name       STRING,
    business_key_hash STRING    NOT NULL,
    row_hash          STRING    NOT NULL,
    load_datetime     TIMESTAMP NOT NULL,
    update_datetime   TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.water_intake_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY startDate, sourceName
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM health_dw.bronze.stg_apple_health_water
    WHERE startDate IS NOT NULL
)
SELECT
    year(to_timestamp(startDate)) * 10000
        + month(to_timestamp(startDate)) * 100
        + dayofmonth(to_timestamp(startDate))             AS sk_date,
    lpad(hour(to_timestamp(startDate)), 2, '0')
        || lpad(minute(to_timestamp(startDate)), 2, '0')  AS sk_time,
    to_timestamp(startDate)                                AS timestamp,
    CAST(value AS DOUBLE)                                  AS water_ml,
    sourceName                                             AS source_name,
    sha2(
        concat_ws('||',
            coalesce(cast(startDate AS STRING), ''),
            coalesce(sourceName, '')
        ), 256
    )                                                      AS business_key_hash,
    sha2(
        coalesce(cast(value AS STRING), ''), 256
    )                                                      AS row_hash,
    current_timestamp()                                    AS load_datetime
FROM deduped
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO health_dw.silver.water_intake AS target
USING health_dw.silver.water_intake_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.sk_date           = source.sk_date,
        target.sk_time           = source.sk_time,
        target.timestamp         = source.timestamp,
        target.water_ml          = source.water_ml,
        target.source_name       = source.source_name,
        target.row_hash          = source.row_hash,
        target.update_datetime   = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        sk_date, sk_time, timestamp, water_ml, source_name,
        business_key_hash, row_hash, load_datetime, update_datetime
    )
    VALUES (
        source.sk_date, source.sk_time, source.timestamp,
        source.water_ml, source.source_name,
        source.business_key_hash, source.row_hash,
        current_timestamp(), current_timestamp()
    );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.water_intake_staging;
