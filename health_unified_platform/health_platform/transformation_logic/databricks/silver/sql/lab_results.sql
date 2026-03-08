-- =============================================================================
-- lab_results.sql
-- Silver: Manual lab test results (blood panels, microbiome)
--
-- Source: health_dw.silver.lab_results_import (loaded via import_manual_data.py)
-- Note: Lab results come from YAML files, not Autoloader.
--
-- Business key: business_key_hash (test_id + marker_name)
-- Change detection: row_hash over value_numeric, status
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.lab_results (
    test_id             STRING,
    test_date           DATE,
    test_type           STRING,
    test_name           STRING,
    lab_name            STRING,
    lab_accreditation   STRING,
    marker_name         STRING,
    marker_category     STRING,
    value_numeric       DOUBLE,
    value_text          STRING,
    unit                STRING,
    reference_min       DOUBLE,
    reference_max       DOUBLE,
    reference_direction STRING,
    status              STRING,
    business_key_hash   STRING    NOT NULL,
    row_hash            STRING    NOT NULL,
    load_datetime       TIMESTAMP NOT NULL,
    update_datetime     TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.lab_results_staging AS
SELECT
    test_id,
    to_date(test_date)                                    AS test_date,
    test_type,
    test_name,
    lab_name,
    lab_accreditation,
    marker_name,
    marker_category,
    CAST(value_numeric AS DOUBLE)                         AS value_numeric,
    value_text,
    unit,
    CAST(reference_min AS DOUBLE)                         AS reference_min,
    CAST(reference_max AS DOUBLE)                         AS reference_max,
    reference_direction,
    status,
    sha2(
        concat_ws('||',
            coalesce(test_id, ''),
            coalesce(marker_name, '')
        ), 256
    )                                                     AS business_key_hash,
    sha2(
        concat_ws('||',
            coalesce(cast(value_numeric AS STRING), ''),
            coalesce(value_text, ''),
            coalesce(status, '')
        ), 256
    )                                                     AS row_hash,
    current_timestamp()                                   AS load_datetime
FROM health_dw.silver.lab_results_import;

-- COMMAND ----------

MERGE INTO health_dw.silver.lab_results AS target
USING health_dw.silver.lab_results_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.test_id             = source.test_id,
        target.test_date           = source.test_date,
        target.test_type           = source.test_type,
        target.test_name           = source.test_name,
        target.lab_name            = source.lab_name,
        target.lab_accreditation   = source.lab_accreditation,
        target.marker_name         = source.marker_name,
        target.marker_category     = source.marker_category,
        target.value_numeric       = source.value_numeric,
        target.value_text          = source.value_text,
        target.unit                = source.unit,
        target.reference_min       = source.reference_min,
        target.reference_max       = source.reference_max,
        target.reference_direction = source.reference_direction,
        target.status              = source.status,
        target.row_hash            = source.row_hash,
        target.update_datetime     = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        test_id, test_date, test_type, test_name, lab_name, lab_accreditation,
        marker_name, marker_category, value_numeric, value_text, unit,
        reference_min, reference_max, reference_direction, status,
        business_key_hash, row_hash, load_datetime, update_datetime
    )
    VALUES (
        source.test_id, source.test_date, source.test_type, source.test_name,
        source.lab_name, source.lab_accreditation, source.marker_name,
        source.marker_category, source.value_numeric, source.value_text,
        source.unit, source.reference_min, source.reference_max,
        source.reference_direction, source.status,
        source.business_key_hash, source.row_hash,
        current_timestamp(), current_timestamp()
    );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.lab_results_staging;
