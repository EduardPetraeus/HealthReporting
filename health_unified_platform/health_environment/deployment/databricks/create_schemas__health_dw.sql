-- Ensure we are working in the right context
USE CATALOG health_dw;

-- Create schemas for the Medallion layers
CREATE SCHEMA IF NOT EXISTS bronze 
  COMMENT 'Landing area for raw-ish health data';

CREATE SCHEMA IF NOT EXISTS silver 
  COMMENT 'Cleaned and standardized health records';

CREATE SCHEMA IF NOT EXISTS gold   
  COMMENT 'Business-ready views for Power BI reporting';