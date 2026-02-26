# TODO

## Configuration

- [ ] **`databricks.yml` workspace URLs** — replace placeholder hosts with real Databricks workspace URLs for `dev` and `prd` targets in `health_unified_platform/health_environment/deployment/databricks/databricks.yml`
- [ ] **`.env.example`** — document required environment variables (Oura OAuth client ID/secret, Databricks host, token, catalog name, `HEALTH_ENV`)

## Databricks Framework — Complete Coverage

Work in progress. Currently only example configs exist.

- [ ] **Source YAML configs** — 2 / 27 configured in `health_environment/config/databricks/sources/`
- [ ] **Silver SQL transforms** — 1 / 18 implemented in `transformation_logic/databricks/silver/sql/`
- [ ] **Gold configs + SQL** — 1 entity in `health_environment/config/databricks/gold/` + `transformation_logic/databricks/gold/sql/`

See `README.md` files in each folder for the specific remaining items.

## Apple Health Connector

- [ ] **Workout elements** — XML contains `Workout`, `WorkoutEvent`, `WorkoutStatistics` elements that are currently ignored. Add parsing alongside `Record` elements.
- [ ] **Partition consistency** — Apple Health uses `domain/type/year=YYYY/`, Oura uses `year=YYYY/month=MM/day=DD/`. Consider aligning to one scheme.
- [ ] **End-to-end wrapper script** — single shell script that runs: XML → parquet → bronze ingestion → silver merge for all Apple Health types
- [ ] **State file** — track last ingested export date (`~/.config/health_reporting/apple_health_state.json`) to enable true incremental runs

## Quality & Testing

- [ ] **dbt tests** — add `schema.yml` with not-null, unique, accepted-values tests per silver entity
- [ ] **Freshness checks** — validate last ingested timestamp per source (bronze → silver lag)
- [ ] **Row count reconciliation** — bronze vs silver row counts after each merge run

## Cleanup

- [ ] **`health_environment/deployment/databricks/`** — catalog/schema DDL scripts (`create_catalog__health_dw.sql`, `create_schemas__health_dw.sql`) may be superseded by `init.py` — consider archiving
