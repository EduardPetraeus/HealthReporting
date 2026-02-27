-- =============================================================================
-- blood_oxygen.sql
-- Silver: Oura daily blood oxygen (SpO2) summary
--
-- Source: health_dw.bronze.stg_oura_blood_oxygen
--
-- Business key: (id, day)
-- Change detection: row_hash over measurement_type, spo2_percentage, breathing_disturbance_index
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.blood_oxygen (
    sk_date                     INTEGER   NOT NULL,
    id                          STRING    NOT NULL,
    day                         DATE      NOT NULL,
    breathing_disturbance_index DOUBLE,
    measurement_type            STRING    NOT NULL,
    spo2_percentage             DOUBLE,
    row_hash                    STRING    NOT NULL,
    load_datetime               TIMESTAMP NOT NULL,
    update_datetime             TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.blood_oxygen_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id, day ORDER BY _ingested_at DESC) AS rn
    FROM health_dw.bronze.stg_oura_blood_oxygen
    WHERE id IS NOT NULL
)
SELECT
    year(day) * 10000 + month(day) * 100 + dayofmonth(day) AS sk_date,
    id,
    day,
    breathing_disturbance_index,
    measurement_type_key                                    AS measurement_type,
    CAST(measurement_value AS DOUBLE)                       AS spo2_percentage,
    sha2(concat_ws('||',
        measurement_type_key,
        CAST(measurement_value AS STRING),
        CAST(breathing_disturbance_index AS STRING)
    ), 256)                                                 AS row_hash,
    current_timestamp()                                     AS load_datetime
FROM deduped
LATERAL VIEW explode(from_json(spo2_percentage, 'map<string,double>')) AS measurement_type_key, measurement_value
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO health_dw.silver.blood_oxygen AS target
USING health_dw.silver.blood_oxygen_staging AS source
ON target.id = source.id AND target.day = source.day

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.sk_date                      = source.sk_date,
        target.id                           = source.id,
        target.day                          = source.day,
        target.breathing_disturbance_index  = source.breathing_disturbance_index,
        target.measurement_type             = source.measurement_type,
        target.spo2_percentage              = source.spo2_percentage,
        target.row_hash                     = source.row_hash,
        target.update_datetime              = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (sk_date, id, day, breathing_disturbance_index, measurement_type,
            spo2_percentage, row_hash, load_datetime, update_datetime)
    VALUES (source.sk_date, source.id, source.day, source.breathing_disturbance_index,
            source.measurement_type, source.spo2_percentage, source.row_hash,
            current_timestamp(), current_timestamp());

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.blood_oxygen_staging;
