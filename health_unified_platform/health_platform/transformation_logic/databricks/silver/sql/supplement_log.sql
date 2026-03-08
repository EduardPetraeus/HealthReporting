-- =============================================================================
-- supplement_log.sql
-- Silver: Manual supplement protocol log
--
-- Source: health_dw.bronze.stg_manual_supplements (YAML import)
-- Note: This table is populated via import_manual_data.py, not Autoloader.
--       The MERGE pattern is kept for consistency.
--
-- Business key: business_key_hash (supplement_name + start_date)
-- Change detection: row_hash over dose, frequency, status
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.supplement_log (
    supplement_name   STRING,
    dose              DOUBLE,
    unit              STRING,
    frequency         STRING,
    timing            STRING,
    product           STRING,
    start_date        DATE,
    end_date          DATE,
    status            STRING,
    target            STRING,
    notes             STRING,
    business_key_hash STRING    NOT NULL,
    row_hash          STRING    NOT NULL,
    load_datetime     TIMESTAMP NOT NULL,
    update_datetime   TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.supplement_log_staging AS
SELECT
    supplement_name,
    CAST(dose AS DOUBLE)                                  AS dose,
    unit,
    frequency,
    timing,
    product,
    to_date(start_date)                                   AS start_date,
    to_date(end_date)                                     AS end_date,
    status,
    target,
    notes,
    sha2(
        concat_ws('||',
            coalesce(supplement_name, ''),
            coalesce(cast(start_date AS STRING), '')
        ), 256
    )                                                     AS business_key_hash,
    sha2(
        concat_ws('||',
            coalesce(cast(dose AS STRING), ''),
            coalesce(unit, ''),
            coalesce(frequency, ''),
            coalesce(status, '')
        ), 256
    )                                                     AS row_hash,
    current_timestamp()                                   AS load_datetime
FROM health_dw.silver.supplement_log_import;

-- COMMAND ----------

MERGE INTO health_dw.silver.supplement_log AS target
USING health_dw.silver.supplement_log_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.supplement_name = source.supplement_name,
        target.dose            = source.dose,
        target.unit            = source.unit,
        target.frequency       = source.frequency,
        target.timing          = source.timing,
        target.product         = source.product,
        target.start_date      = source.start_date,
        target.end_date        = source.end_date,
        target.status          = source.status,
        target.target          = source.target,
        target.notes           = source.notes,
        target.row_hash        = source.row_hash,
        target.update_datetime = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        supplement_name, dose, unit, frequency, timing, product,
        start_date, end_date, status, target, notes,
        business_key_hash, row_hash, load_datetime, update_datetime
    )
    VALUES (
        source.supplement_name, source.dose, source.unit, source.frequency,
        source.timing, source.product, source.start_date, source.end_date,
        source.status, source.target, source.notes,
        source.business_key_hash, source.row_hash,
        current_timestamp(), current_timestamp()
    );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.supplement_log_staging;
