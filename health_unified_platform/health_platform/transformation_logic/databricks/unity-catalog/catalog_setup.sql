-- =============================================================================
-- catalog_setup.sql
-- Unity Catalog DDL: catalog, schemas, and grants for health_dw
--
-- Run this once per environment (dev/prd) to bootstrap the catalog structure.
-- Idempotent: all statements use IF NOT EXISTS.
-- =============================================================================

-- Create the main catalog
CREATE CATALOG IF NOT EXISTS health_dw;

COMMENT ON CATALOG health_dw IS
    'Unified Health Platform — personal health data warehouse (medallion architecture)';

USE CATALOG health_dw;

-- =============================================================================
-- Schemas (one per medallion layer + audit)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bronze
    COMMENT 'Landing area for raw health data from all sources (Autoloader ingestion)';

CREATE SCHEMA IF NOT EXISTS silver
    COMMENT 'Cleaned, deduplicated, and standardized health records (MERGE pattern)';

CREATE SCHEMA IF NOT EXISTS gold
    COMMENT 'Business-ready views and tables for BI dashboards and reporting';

CREATE SCHEMA IF NOT EXISTS audit
    COMMENT 'Pipeline audit logs, data quality results, and lineage tracking';

-- =============================================================================
-- Grants — workspace-level access control
-- =============================================================================

-- Data engineers get full access to all layers
GRANT USE CATALOG ON CATALOG health_dw TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA health_dw.bronze TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA health_dw.silver TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA health_dw.gold TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA health_dw.audit TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA health_dw.bronze TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA health_dw.silver TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA health_dw.gold TO `data_engineers`;
GRANT ALL PRIVILEGES ON SCHEMA health_dw.audit TO `data_engineers`;

-- BI consumers get read-only access to gold
GRANT USE CATALOG ON CATALOG health_dw TO `bi_consumers`;
GRANT USE SCHEMA ON SCHEMA health_dw.gold TO `bi_consumers`;
GRANT SELECT ON SCHEMA health_dw.gold TO `bi_consumers`;

-- Service principals for pipeline execution
GRANT USE CATALOG ON CATALOG health_dw TO `health_pipeline_sp`;
GRANT USE SCHEMA ON SCHEMA health_dw.bronze TO `health_pipeline_sp`;
GRANT USE SCHEMA ON SCHEMA health_dw.silver TO `health_pipeline_sp`;
GRANT USE SCHEMA ON SCHEMA health_dw.gold TO `health_pipeline_sp`;
GRANT USE SCHEMA ON SCHEMA health_dw.audit TO `health_pipeline_sp`;
GRANT ALL PRIVILEGES ON SCHEMA health_dw.bronze TO `health_pipeline_sp`;
GRANT ALL PRIVILEGES ON SCHEMA health_dw.silver TO `health_pipeline_sp`;
GRANT ALL PRIVILEGES ON SCHEMA health_dw.gold TO `health_pipeline_sp`;
GRANT ALL PRIVILEGES ON SCHEMA health_dw.audit TO `health_pipeline_sp`;
