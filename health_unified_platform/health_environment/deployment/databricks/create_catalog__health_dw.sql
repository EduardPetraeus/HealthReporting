-- Creates the catalog only if it doesn't exist to prevent pipeline crashes
CREATE CATALOG IF NOT EXISTS health_dw;

COMMENT ON CATALOG health_dw IS 'Main catalog for the Unified Health Platform - Managed by Claus';