-- =============================================================================
-- table_definitions.sql
-- Unity Catalog DDL: CREATE TABLE statements for key silver and gold tables
--
-- These definitions serve as the schema contract for the Databricks deployment.
-- All tables use Delta format with change data feed enabled where applicable.
-- =============================================================================

USE CATALOG health_dw;

-- =============================================================================
-- Silver tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.daily_sleep (
    sk_date         INTEGER   NOT NULL,
    id              STRING    NOT NULL,
    day             DATE      NOT NULL,
    sleep_score     BIGINT,
    timestamp       TIMESTAMP,
    deep_sleep      DOUBLE,
    efficiency      DOUBLE,
    latency         DOUBLE,
    rem_sleep       DOUBLE,
    restfulness     DOUBLE,
    timing          DOUBLE,
    total_sleep     DOUBLE,
    row_hash        STRING    NOT NULL,
    load_datetime   TIMESTAMP NOT NULL,
    update_datetime TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS silver.daily_activity (
    sk_date           INTEGER   NOT NULL,
    id                STRING    NOT NULL,
    day               DATE      NOT NULL,
    activity_score    BIGINT,
    steps             BIGINT,
    active_calories   BIGINT,
    total_calories    BIGINT,
    low_activity      BIGINT,
    medium_activity   BIGINT,
    high_activity     BIGINT,
    row_hash          STRING    NOT NULL,
    load_datetime     TIMESTAMP NOT NULL,
    update_datetime   TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS silver.daily_readiness (
    sk_date                INTEGER   NOT NULL,
    id                     STRING    NOT NULL,
    day                    DATE      NOT NULL,
    readiness_score        BIGINT,
    temperature_deviation  DOUBLE,
    timestamp              TIMESTAMP,
    row_hash               STRING    NOT NULL,
    load_datetime          TIMESTAMP NOT NULL,
    update_datetime        TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS silver.heart_rate (
    source_system   STRING    NOT NULL,
    record_id       STRING    NOT NULL,
    recorded_at     TIMESTAMP,
    heart_rate_bpm  DOUBLE,
    _updated_at     TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS silver.blood_pressure (
    sk_date           INTEGER   NOT NULL,
    sk_time           STRING,
    datetime          TIMESTAMP NOT NULL,
    systolic          DOUBLE,
    diastolic         DOUBLE,
    pulse             DOUBLE,
    source_name       STRING,
    business_key_hash STRING    NOT NULL,
    row_hash          STRING    NOT NULL,
    load_datetime     TIMESTAMP NOT NULL,
    update_datetime   TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS silver.weight (
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
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS silver.daily_meal (
    sk_date         INTEGER   NOT NULL,
    date            DATE      NOT NULL,
    meal_type       STRING,
    food_name       STRING,
    calories        DOUBLE,
    protein         DOUBLE,
    carbs           DOUBLE,
    fat             DOUBLE,
    carbs_fiber     DOUBLE,
    carbs_sugar     DOUBLE,
    row_hash        STRING    NOT NULL,
    load_datetime   TIMESTAMP NOT NULL,
    update_datetime TIMESTAMP NOT NULL
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver.step_count (
    sk_date           INTEGER   NOT NULL,
    sk_time           STRING    NOT NULL,
    timestamp         TIMESTAMP,
    end_timestamp     TIMESTAMP,
    duration_seconds  DOUBLE,
    step_count        INTEGER,
    source_name       STRING,
    business_key_hash STRING    NOT NULL,
    row_hash          STRING    NOT NULL,
    load_datetime     TIMESTAMP NOT NULL,
    update_datetime   TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS silver.workout (
    sk_date           INTEGER   NOT NULL,
    day               DATE      NOT NULL,
    workout_id        STRING,
    activity          STRING,
    intensity         STRING,
    calories          DOUBLE,
    distance_meters   DOUBLE,
    start_datetime    TIMESTAMP,
    end_datetime      TIMESTAMP,
    duration_seconds  DOUBLE,
    label             STRING,
    source            STRING,
    business_key_hash STRING    NOT NULL,
    row_hash          STRING    NOT NULL,
    load_datetime     TIMESTAMP NOT NULL,
    update_datetime   TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS silver.daily_stress (
    sk_date           INTEGER   NOT NULL,
    day               DATE      NOT NULL,
    day_summary       STRING,
    stress_high       INTEGER,
    recovery_high     INTEGER,
    business_key_hash STRING    NOT NULL,
    row_hash          STRING    NOT NULL,
    load_datetime     TIMESTAMP NOT NULL,
    update_datetime   TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS silver.lab_results (
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

CREATE TABLE IF NOT EXISTS silver.supplement_log (
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

-- =============================================================================
-- Audit tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit.pipeline_log (
    log_id          STRING    NOT NULL,
    pipeline_name   STRING    NOT NULL,
    layer           STRING    NOT NULL,
    source_system   STRING,
    table_name      STRING,
    operation       STRING,
    rows_affected   BIGINT,
    started_at      TIMESTAMP NOT NULL,
    completed_at    TIMESTAMP,
    status          STRING    DEFAULT 'RUNNING',
    error_message   STRING
)
USING DELTA;
