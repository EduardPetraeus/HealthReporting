# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Runner
# MAGIC
# MAGIC Generic runner for gold layer views and tables.
# MAGIC Reads YAML entity configs from `config/gold/` and executes
# MAGIC `CREATE OR REPLACE VIEW` or `CREATE OR REPLACE TABLE` SQL files.
# MAGIC
# MAGIC **Widget parameters**
# MAGIC | Parameter | Description |
# MAGIC |-----------|-------------|
# MAGIC | `entity_name` | Run one gold entity by name; leave empty to run all |
# MAGIC | `config_root` | Workspace path containing gold YAML configs |
# MAGIC | `sql_root` | Workspace path containing gold SQL files |
# MAGIC
# MAGIC **Gold config YAML structure:**
# MAGIC ```yaml
# MAGIC name: daily_heart_rate_summary
# MAGIC type: view          # view | table
# MAGIC target: health_dw.gold.daily_heart_rate_summary
# MAGIC sql_file: daily_heart_rate_summary.sql
# MAGIC ```
# MAGIC
# MAGIC **Gold SQL file template variables:**
# MAGIC ```sql
# MAGIC CREATE OR REPLACE VIEW {target} AS
# MAGIC SELECT ...
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("entity_name", "", "Entity Name (empty = all)")
dbutils.widgets.text(
    "config_root",
    "/Workspace/Shared/health_platform/config/gold",
    "Config Root",
)
dbutils.widgets.text(
    "sql_root",
    "/Workspace/Shared/health_platform/notebooks/gold/sql",
    "SQL Root",
)

entity_name = dbutils.widgets.get("entity_name").strip()
config_root = dbutils.widgets.get("config_root").strip()
sql_root    = dbutils.widgets.get("sql_root").strip()

# COMMAND ----------

import glob as _glob
import yaml

VALID_ENTITY_TYPES = {"view", "table"}


def load_gold_configs(config_root: str, entity_name: str = "") -> list:
    """
    Return a list of gold entity config dicts.

    If entity_name is provided, loads only that config.
    Otherwise, loads all *.yml files in config_root.
    """
    if entity_name:
        paths = [f"{config_root}/{entity_name}.yml"]
    else:
        paths = sorted(_glob.glob(f"{config_root}/*.yml"))

    configs = []
    for path in paths:
        with open(path) as f:
            configs.append(yaml.safe_load(f))

    return configs


def run_gold_entity(config: dict, sql_root: str) -> None:
    """
    Load and execute the gold SQL for one entity config.

    Template variables substituted into the SQL file:
        {target}  — fully-qualified gold table or view name
    """
    name        = config["name"]
    target      = config["target"]
    entity_type = config.get("type", "view").lower()
    sql_file    = config["sql_file"]

    if entity_type not in VALID_ENTITY_TYPES:
        raise ValueError(
            f"Unknown type '{entity_type}' for entity '{name}'. "
            f"Expected one of: {VALID_ENTITY_TYPES}"
        )

    sql_path = f"{sql_root}/{sql_file}"

    print(f"\n{'=' * 60}")
    print(f"Entity:   {name}")
    print(f"Type:     {entity_type}")
    print(f"Target:   {target}")
    print(f"SQL file: {sql_file}")
    print(f"{'=' * 60}\n")

    with open(sql_path) as f:
        sql_template = f.read()

    sql = sql_template.format(target=target)

    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for i, statement in enumerate(statements, 1):
        print(f"[{name}] Executing statement {i}/{len(statements)}…")
        spark.sql(statement)

    print(f"[{name}] Done.")


# COMMAND ----------

configs = load_gold_configs(config_root, entity_name)

if not configs:
    label = f"entity '{entity_name}'" if entity_name else f"config_root '{config_root}'"
    raise ValueError(f"No gold configs found for {label}.")

print(f"Entities to process: {[c['name'] for c in configs]}\n")

for cfg in configs:
    run_gold_entity(cfg, sql_root)

print(f"\nAll done. Processed {len(configs)} gold entity/entities.")
