-- =============================================================================
-- daily_activity.sql
-- Silver: Oura daily activity metrics
--
-- Source: workspace.default.oura_daily_activity
-- TODO: Update source to health_dw.bronze.stg_oura_daily_activity when bronze is ready
--
-- Business key: (id, day)
-- Change detection: row_hash over score, steps, calorie, and contributor fields
-- =============================================================================

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS health_dw.silver.daily_activity (
    sk_date                     INTEGER   NOT NULL,
    id                          STRING    NOT NULL,
    day                         DATE      NOT NULL,
    activity_score              BIGINT,
    steps                       BIGINT,
    equivalent_walking_distance BIGINT,
    inactivity_alerts           BIGINT,
    target_calories             BIGINT,
    active_calories             BIGINT,
    meet_daily_targets          DOUBLE,
    move_every_hour             DOUBLE,
    recovery_time               DOUBLE,
    stay_active                 DOUBLE,
    training_frequency          DOUBLE,
    training_volume             DOUBLE,
    row_hash                    STRING    NOT NULL,
    load_datetime               TIMESTAMP NOT NULL,
    update_datetime             TIMESTAMP NOT NULL
)
USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TABLE health_dw.silver.daily_activity_staging AS
WITH deduped_activity AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id, day
            ORDER BY id, day
        ) AS rn
    FROM workspace.default.oura_daily_activity
    WHERE id IS NOT NULL
)
SELECT
    year(day) * 10000 + month(day) * 100 + dayofmonth(day)             AS sk_date,
    id,
    day,
    score                                                               AS activity_score,
    steps,
    equivalent_walking_distance,
    inactivity_alerts,
    target_calories,
    active_calories,
    CAST(get_json_object(contributors, '$.meet_daily_targets') AS DOUBLE) AS meet_daily_targets,
    CAST(get_json_object(contributors, '$.move_every_hour')    AS DOUBLE) AS move_every_hour,
    CAST(get_json_object(contributors, '$.recovery_time')      AS DOUBLE) AS recovery_time,
    CAST(get_json_object(contributors, '$.stay_active')        AS DOUBLE) AS stay_active,
    CAST(get_json_object(contributors, '$.training_frequency') AS DOUBLE) AS training_frequency,
    CAST(get_json_object(contributors, '$.training_volume')    AS DOUBLE) AS training_volume,
    sha2(
        concat_ws('||',
            score, steps, equivalent_walking_distance, inactivity_alerts,
            target_calories, active_calories,
            get_json_object(contributors, '$.meet_daily_targets'),
            get_json_object(contributors, '$.move_every_hour'),
            get_json_object(contributors, '$.recovery_time'),
            get_json_object(contributors, '$.stay_active'),
            get_json_object(contributors, '$.training_frequency'),
            get_json_object(contributors, '$.training_volume')
        ), 256
    )                                                                   AS row_hash,
    current_timestamp()                                                 AS load_datetime
FROM deduped_activity
WHERE rn = 1;

-- COMMAND ----------

MERGE INTO health_dw.silver.daily_activity AS target
USING health_dw.silver.daily_activity_staging AS source
ON target.id = source.id AND target.day = source.day

WHEN MATCHED AND target.row_hash <> source.row_hash THEN
    UPDATE SET
        target.sk_date                     = source.sk_date,
        target.activity_score              = source.activity_score,
        target.steps                       = source.steps,
        target.equivalent_walking_distance = source.equivalent_walking_distance,
        target.inactivity_alerts           = source.inactivity_alerts,
        target.target_calories             = source.target_calories,
        target.active_calories             = source.active_calories,
        target.meet_daily_targets          = source.meet_daily_targets,
        target.move_every_hour             = source.move_every_hour,
        target.recovery_time               = source.recovery_time,
        target.stay_active                 = source.stay_active,
        target.training_frequency          = source.training_frequency,
        target.training_volume             = source.training_volume,
        target.row_hash                    = source.row_hash,
        target.update_datetime             = current_timestamp()

WHEN NOT MATCHED THEN
    INSERT (
        sk_date, id, day, activity_score, steps, equivalent_walking_distance,
        inactivity_alerts, target_calories, active_calories, meet_daily_targets,
        move_every_hour, recovery_time, stay_active, training_frequency,
        training_volume, row_hash, load_datetime, update_datetime
    )
    VALUES (
        source.sk_date, source.id, source.day, source.activity_score, source.steps,
        source.equivalent_walking_distance, source.inactivity_alerts, source.target_calories,
        source.active_calories, source.meet_daily_targets, source.move_every_hour,
        source.recovery_time, source.stay_active, source.training_frequency,
        source.training_volume, source.row_hash, current_timestamp(), current_timestamp()
    );

-- COMMAND ----------

DROP TABLE IF EXISTS health_dw.silver.daily_activity_staging;
