# TODO

## Configuration

- [ ] **`databricks.yml` workspace URLs** — replace placeholder hosts with real Databricks workspace URLs for `dev` and `prd` targets in `health_unified_platform/databricks_framework/bundles/databricks.yml` (lines 70 + 77)
- [ ] **`.env.example`** — document required environment variables (Oura OAuth client ID/secret, Databricks host, token, catalog name, `HEALTH_ENV`)

## Databricks Framework — Complete Coverage

Currently only example configs exist. Need to add for all sources:

- [ ] Source YAML configs for all 27 bronze sources in `databricks_framework/config/sources/`
- [ ] Silver SQL transforms for all 18 entities in `databricks_framework/notebooks/silver/sql/`
- [ ] Gold configs and SQL for remaining gold views in `databricks_framework/config/gold/` + `notebooks/gold/sql/`

## Quality & Testing

- [ ] **dbt tests** — add `schema.yml` with not-null, unique, accepted-values tests per silver entity
- [ ] **Freshness checks** — validate last ingested timestamp per source (bronze → silver lag)
- [ ] **Row count reconciliation** — bronze vs silver row counts after each merge run

## Cleanup

- [ ] **`health_environment/deployment/databricks/`** — legacy catalog/schema DDL, consider moving to `archive/`
- [ ] **`databricks_framework/bundles/workflows/`** — clarify relationship between `bundles/workflows/` (empty) and `databricks_framework/workflows/` (active YAMLs)
