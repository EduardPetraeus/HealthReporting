-- =============================================================================
-- weight.sql
-- Silver: Withings body composition measurements
--
-- Source: health_dw.bronze.stg_withings_weight
--
-- Business key: sha2(datetime || weight_kg || fat_mass_kg || bone_mass_kg || muscle_mass_kg || hydration_kg)
-- Change detection: row_hash over all mass fields and comments
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.weight (
    sk_date           INTEGER   NOT NULL,
    sk_time           STRING    NOT NULL,
    datetime          TIMESTAMP NOT NULL,
    weight_kg         DOUBLE    NOT NULL,
    fat_mass_kg       DOUBLE,
    bone_mass_kg      DOUBLE,
    muscle_mass_kg    DOUBLE,
    hydration_kg      DOUBLE,
    business_key_hash STRING    NOT NULL,
    row_hash          STRING    NOT NULL,
    load_datetime     TIMESTAMP NOT NULL,
    update_datetime   TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.weight_staging AS
WITH deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY datetime, weight_kg
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM health_dw.bronze.stg_withings_weight
    WHERE datetime IS NOT NULL
      AND weight_kg IS NOT NULL
)
SELECT
    year(datetime) * 10000 + month(datetime) * 100 + dayofmonth(datetime) AS sk_date,
    lpad(hour(datetime), 2, '0') || lpad(minute(datetime), 2, '0')        AS sk_time,
    datetime,
    weight_kg,
    fat_mass_kg,
    bone_mass_kg,
    muscle_mass_kg,
    hydration_kg,
    sha2(
        concat_ws('||',
            coalesce(cast(datetime AS STRING), ''),
            coalesce(cast(weight_kg AS STRING), ''),
            coalesce(cast(fat_mass_kg AS STRING), ''),
            coalesce(cast(bone_mass_kg AS STRING), ''),
            coalesce(cast(muscle_mass_kg AS STRING), ''),
            coalesce(cast(hydration_kg AS STRING), '')
        ), 256
    )                                                                      AS business_key_hash,
    sha2(
        concat_ws('||',
            coalesce(cast(weight_kg AS STRING), ''),
            coalesce(cast(fat_mass_kg AS STRING), ''),
            coalesce(cast(bone_mass_kg AS STRING), ''),
            coalesce(cast(muscle_mass_kg AS STRING), ''),
            coalesce(cast(hydration_kg AS STRING), '')
        ), 256
    )                                                                      AS row_hash,
    current_timestamp()                                                    AS load_datetime
FROM deduped
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO health_dw.silver.weight AS target
USING health_dw.silver.weight_staging AS source
ON target.business_key_hash = source.business_key_hash

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.sk_date         = source.sk_date,
        target.sk_time         = source.sk_time,
        target.datetime        = source.datetime,
        target.weight_kg       = source.weight_kg,
        target.fat_mass_kg     = source.fat_mass_kg,
        target.bone_mass_kg    = source.bone_mass_kg,
        target.muscle_mass_kg  = source.muscle_mass_kg,
        target.hydration_kg    = source.hydration_kg,
        target.row_hash        = source.row_hash,
        target.update_datetime = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        sk_date, sk_time, datetime, weight_kg, fat_mass_kg, bone_mass_kg,
        muscle_mass_kg, hydration_kg, business_key_hash, row_hash, load_datetime, update_datetime
    )
    VALUES (
        source.sk_date, source.sk_time, source.datetime, source.weight_kg,
        source.fat_mass_kg, source.bone_mass_kg, source.muscle_mass_kg, source.hydration_kg,
        source.business_key_hash, source.row_hash, current_timestamp(), current_timestamp()
    );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.weight_staging;
