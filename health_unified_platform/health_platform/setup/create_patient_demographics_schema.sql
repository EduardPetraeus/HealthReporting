-- Patient Demographics Silver Schema
-- Static patient data from manually curated YAML.
-- Run after create_lab_and_supplements_schema.sql
--
-- Tables:
--   silver.patient_demographics  — singleton row (DOB, sex, height, lifestyle)
--   silver.medical_history       — one row per condition
--   silver.vaccinations          — one row per vaccine
--   silver.family_history        — one row per relation + condition
--   silver.device_registry       — one row per device

CREATE SCHEMA IF NOT EXISTS silver;

-- =============================================================================
-- SILVER: Patient Demographics — singleton patient record
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.patient_demographics (
    patient_id          VARCHAR NOT NULL DEFAULT 'patient_001',
    date_of_birth       DATE NOT NULL,
    sex                 VARCHAR NOT NULL,
    height_cm           INTEGER,
    smoking_current     BOOLEAN,
    smoking_history     VARCHAR,
    alcohol_level       VARCHAR,
    alcohol_frequency   VARCHAR,
    alcohol_notes       VARCHAR,
    business_key_hash   VARCHAR NOT NULL,
    row_hash            VARCHAR NOT NULL,
    load_datetime       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_datetime     TIMESTAMP
);

-- =============================================================================
-- SILVER: Medical History — one row per condition
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.medical_history (
    condition           VARCHAR NOT NULL,
    status              VARCHAR NOT NULL,
    notes               VARCHAR,
    business_key_hash   VARCHAR NOT NULL,
    row_hash            VARCHAR NOT NULL,
    load_datetime       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_datetime     TIMESTAMP
);

-- =============================================================================
-- SILVER: Vaccinations — one row per vaccine
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.vaccinations (
    vaccine             VARCHAR NOT NULL,
    coverage            VARCHAR,
    year_administered   INTEGER,
    notes               VARCHAR,
    business_key_hash   VARCHAR NOT NULL,
    row_hash            VARCHAR NOT NULL,
    load_datetime       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_datetime     TIMESTAMP
);

-- =============================================================================
-- SILVER: Family History — one row per relation + condition
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.family_history (
    relation            VARCHAR NOT NULL,
    condition           VARCHAR NOT NULL,
    status              VARCHAR,
    age_at_death_approx VARCHAR,
    notes               VARCHAR,
    business_key_hash   VARCHAR NOT NULL,
    row_hash            VARCHAR NOT NULL,
    load_datetime       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_datetime     TIMESTAMP
);

-- =============================================================================
-- SILVER: Device Registry — one row per device
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.device_registry (
    device              VARCHAR NOT NULL,
    category            VARCHAR NOT NULL,
    data_types          VARCHAR NOT NULL,
    notes               VARCHAR,
    business_key_hash   VARCHAR NOT NULL,
    row_hash            VARCHAR NOT NULL,
    load_datetime       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_datetime     TIMESTAMP
);
