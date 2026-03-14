-- Lab Results, Supplement Log, and Genetic Profile Schema
-- DuckDB-compatible DDL
--
-- lab_results:       Periodic biomarker measurements (blood panels, microbiome, etc.)
-- supplement_log:    Active/planned supplement and intervention tracking
-- genetic_profile:   Static genetic findings from genotyping platforms (23andMe, WGS)

-- =============================================================================
-- SILVER: Lab Results — periodic biomarker measurements
-- One row per marker per test. Supports longitudinal tracking across retests.
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.lab_results (
    test_id             VARCHAR NOT NULL,
    test_date           DATE NOT NULL,
    test_type           VARCHAR NOT NULL,
    test_name           VARCHAR,
    lab_name            VARCHAR,
    lab_accreditation   VARCHAR,
    marker_name         VARCHAR NOT NULL,
    marker_category     VARCHAR NOT NULL,
    value_numeric       DOUBLE,
    value_text          VARCHAR,
    unit                VARCHAR,
    reference_min       DOUBLE,
    reference_max       DOUBLE,
    reference_direction VARCHAR,
    status              VARCHAR NOT NULL,
    business_key_hash   VARCHAR NOT NULL,
    row_hash            VARCHAR NOT NULL,
    load_datetime       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_datetime     TIMESTAMP
);

-- =============================================================================
-- SILVER: Supplement Log — intervention tracking
-- One row per supplement. Tracks active/planned/stopped/awaiting_rx status.
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.supplement_log (
    supplement_name     VARCHAR NOT NULL,
    dose                DOUBLE,
    unit                VARCHAR,
    frequency           VARCHAR,
    timing              VARCHAR,
    product             VARCHAR,
    start_date          DATE,
    end_date            DATE,
    status              VARCHAR NOT NULL,
    target              VARCHAR,
    notes               VARCHAR,
    business_key_hash   VARCHAR NOT NULL,
    row_hash            VARCHAR NOT NULL,
    load_datetime       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_datetime     TIMESTAMP
);

-- =============================================================================
-- SILVER: Marker Catalog — canonical marker definitions
-- One row per biomarker. Defines canonical unit, domain, and display metadata.
-- Supports cross-lab normalization via synonyms column.
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.dim_marker_catalog (
    marker_key          VARCHAR NOT NULL PRIMARY KEY,
    display_name        VARCHAR NOT NULL,
    marker_domain       VARCHAR NOT NULL,
    body_system         VARCHAR NOT NULL,
    canonical_unit      VARCHAR,                        -- NULL = dimensionless (e.g. pH, qualitative)
    description         VARCHAR,
    data_type           VARCHAR NOT NULL DEFAULT 'numeric',
    is_log_scale        BOOLEAN NOT NULL DEFAULT false,
    detection_limit     DOUBLE,
    synonyms            VARCHAR,
    load_datetime       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- SILVER: Reference Range — cross-lab reference ranges with temporal validity
-- Multiple rows per marker: one per lab source and optional age/sex strata.
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.dim_reference_range (
    marker_key          VARCHAR NOT NULL,
    source              VARCHAR NOT NULL,
    reference_type      VARCHAR NOT NULL,
    reference_min       DOUBLE,
    reference_max       DOUBLE,
    unit                VARCHAR,                        -- NULL = dimensionless (e.g. pH, qualitative)
    age_group           VARCHAR DEFAULT 'adult',
    sex                 VARCHAR DEFAULT 'male',
    notes               VARCHAR,
    effective_from      DATE,
    effective_to        DATE,
    load_datetime       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (marker_key, source)
);

-- =============================================================================
-- AGENT: Genetic Profile — static genetic findings
-- One row per genetic report/finding. Loaded once, referenced permanently.
-- =============================================================================
CREATE TABLE IF NOT EXISTS agent.genetic_profile (
    category            VARCHAR NOT NULL,
    report_name         VARCHAR NOT NULL,
    result_summary      VARCHAR NOT NULL,
    variant_detected    BOOLEAN,
    gene                VARCHAR,
    snp_id              VARCHAR,
    genotype            VARCHAR,
    clinical_relevance  VARCHAR NOT NULL,
    platform_relevance  VARCHAR,
    related_metrics     VARCHAR,
    load_datetime       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (category, report_name)
);
