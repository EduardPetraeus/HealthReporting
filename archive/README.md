# Archive

Legacy pipeline definitions kept for reference. Not in active use.

## Contents

- `legacy_databricks_pipeline/` — Early Databricks SQL definitions (silver notebooks + table DDL, gold views) built before the metadata-driven framework in `databricks_framework/`. Superseded by `databricks_framework/` + YAML-driven runners.

## Migration status

All SQL from `legacy_databricks_pipeline/` has been ported and the archive copies deleted:

| Legacy file | Migrated to | Status |
|-------------|-------------|--------|
| `silver/notebook/*.sql` + `silver/table/*.sql` | `transformation_logic/databricks/silver/sql/` | ✅ Migrated, archive deleted |
| `gold/view/*.sql` | `transformation_logic/databricks/gold/sql/` | ✅ Migrated, archive deleted |
| `silver/notebook/date.py` | `transformation_logic/databricks/silver/dim_date.py` | ✅ Migrated, archive deleted |
| `silver/notebook/time.py` | `transformation_logic/databricks/silver/dim_time.py` | ✅ Migrated, archive deleted |

The 9 silver `.py` notebooks with `# MAGIC %sql` blocks were also converted to pure `.sql` files (PR #19).
Source references still point to `workspace.default.*` — will be updated to `health_dw.bronze.stg_*` when the bronze layer is ready.
