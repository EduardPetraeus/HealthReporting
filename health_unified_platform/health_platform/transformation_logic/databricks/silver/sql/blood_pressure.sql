-- =============================================================================
-- blood_pressure.sql
-- Silver: Withings blood pressure measurements
--
-- Source: workspace.default.withings_blood_pressure
-- TODO: Update source to health_dw.bronze.stg_withings_blood_pressure when bronze is ready
--
-- Business key: sha2(Date || Systolic || Diastolic)
-- Change detection: row_hash over Systolic, Diastolic, Comments
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.blood_pressure (
    sk_date           INTEGER   NOT NULL,
    sk_time           STRING    NOT NULL,
    datetime          TIMESTAMP NOT NULL,
    systolic          BIGINT    NOT NULL,
    diastolic         BIGINT    NOT NULL,
    comments          STRING,
    business_key_hash STRING    NOT NULL,
    row_hash          STRING    NOT NULL,
    load_datetime     TIMESTAMP NOT NULL,
    update_datetime   TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.blood_pressure_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY Date, Systolic, Diastolic ORDER BY Date) AS rn
    FROM workspace.default.withings_blood_pressure
    WHERE Date IS NOT NULL AND Systolic IS NOT NULL AND Diastolic IS NOT NULL
)
SELECT
    year(Date) * 10000 + month(Date) * 100 + dayofmonth(Date) AS sk_date,
    lpad(hour(Date), 2, '0') || lpad(minute(Date), 2, '0')    AS sk_time,
    Date                                                        AS datetime,
    Systolic                                                    AS systolic,
    Diastolic                                                   AS diastolic,
    Comments                                                    AS comments,
    sha2(concat_ws('||',
        coalesce(cast(Date AS STRING), ''),
        coalesce(cast(Systolic AS STRING), ''),
        coalesce(cast(Diastolic AS STRING), '')
    ), 256)                                                     AS business_key_hash,
    sha2(concat_ws('||',
        coalesce(cast(Systolic AS STRING), ''),
        coalesce(cast(Diastolic AS STRING), ''),
        coalesce(Comments, '')
    ), 256)                                                     AS row_hash,
    current_timestamp()                                         AS load_datetime
FROM deduped
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO health_dw.silver.blood_pressure AS target
USING health_dw.silver.blood_pressure_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.sk_date           = source.sk_date,
        target.sk_time           = source.sk_time,
        target.datetime          = source.datetime,
        target.systolic          = source.systolic,
        target.diastolic         = source.diastolic,
        target.comments          = source.comments,
        target.business_key_hash = source.business_key_hash,
        target.row_hash          = source.row_hash,
        target.update_datetime   = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (sk_date, sk_time, datetime, systolic, diastolic, comments,
            business_key_hash, row_hash, load_datetime, update_datetime)
    VALUES (source.sk_date, source.sk_time, source.datetime, source.systolic,
            source.diastolic, source.comments, source.business_key_hash,
            source.row_hash, current_timestamp(), current_timestamp());

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.blood_pressure_staging;
