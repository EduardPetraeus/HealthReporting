-- DDL for genetics silver tables
-- Run after create_lab_and_supplements_schema.sql

CREATE SCHEMA IF NOT EXISTS silver;

-- Deduplicated health findings from 23andMe PDF reports
CREATE TABLE IF NOT EXISTS silver.genetic_health_findings (
    report_name         VARCHAR NOT NULL,
    category            VARCHAR NOT NULL,
    condition           VARCHAR NOT NULL,
    gene                VARCHAR,
    snp_id              VARCHAR,
    genotype            VARCHAR,
    risk_level          VARCHAR NOT NULL,
    result_summary      VARCHAR NOT NULL,
    variant_detected    BOOLEAN NOT NULL,
    source_file         VARCHAR,
    business_key_hash   VARCHAR NOT NULL,
    row_hash            VARCHAR NOT NULL,
    load_datetime       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_datetime     TIMESTAMP
);

-- Ancestry composition segments from 23andMe CSV export
CREATE TABLE IF NOT EXISTS silver.ancestry_segments (
    ancestry_category   VARCHAR NOT NULL,
    copy                INTEGER NOT NULL,
    chromosome          VARCHAR NOT NULL,
    start_bp            BIGINT NOT NULL,
    end_bp              BIGINT NOT NULL,
    segment_length_bp   BIGINT NOT NULL,
    pct_of_chromosome   DOUBLE,
    source_file         VARCHAR,
    business_key_hash   VARCHAR NOT NULL,
    row_hash            VARCHAR NOT NULL,
    load_datetime       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_datetime     TIMESTAMP
);

-- Family tree members from 23andMe JSON export
CREATE TABLE IF NOT EXISTS silver.family_tree (
    person_id               VARCHAR NOT NULL,
    first_name              VARCHAR,
    last_name               VARCHAR,
    relationship_to_user    VARCHAR NOT NULL,
    generation              INTEGER NOT NULL,
    side                    VARCHAR,
    num_shared_segments     INTEGER NOT NULL DEFAULT 0,
    has_dna_match           BOOLEAN NOT NULL DEFAULT false,
    profile_url             VARCHAR,
    source_file             VARCHAR,
    business_key_hash       VARCHAR NOT NULL,
    row_hash                VARCHAR NOT NULL,
    load_datetime           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_datetime         TIMESTAMP
);
