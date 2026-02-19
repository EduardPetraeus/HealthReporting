# Key Paths

| Path | Purpose |
|---|---|
| `health_unified_platform/health_environment/config/environment_config.yaml` | Data lake root, DB path, env defaults |
| `health_unified_platform/health_environment/config/sources_config.yaml` | Metadata-driven source -> bronze table mapping |
| `health_unified_platform/health_platform/source_connectors/` | Per-source connectors (currently only apple_health) |
| `health_unified_platform/health_platform/transformation_logic/ingestion_engine.py` | Main bronze ingestion engine |
| `health_unified_platform/health_platform/transformation_logic/databricks/silver/` | Silver layer DDL (`table/`) and transformation notebooks (`notebook/`) |
| `health_unified_platform/health_platform/transformation_logic/databricks/gold/view/` | Gold layer views |
| `health_unified_platform/health_environment/deployment/databricks/` | Databricks catalog/schema DDL |
| `legacy_on_premise_dw/` | Deprecated SQL Server + SSIS + Tabular Model (reference only) |