# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Runner
# MAGIC
# MAGIC Generic, config-driven silver transformation notebook.
# MAGIC Reads YAML source configs, substitutes template variables into SQL files,
# MAGIC and executes the resulting statements (MERGE INTO or INSERT INTO … REPLACE WHERE).
# MAGIC
# MAGIC **No transformation logic lives here** — all SQL is in `sql/` files.
# MAGIC This notebook is purely a parameter-substitution and execution engine.
# MAGIC
# MAGIC **Widget parameters**
# MAGIC | Parameter | Description |
# MAGIC |-----------|-------------|
# MAGIC | `source_name` | Run one source by name; leave empty to run all configs in `config_root` |
# MAGIC | `config_root` | Workspace path containing source YAML configs |
# MAGIC | `sql_root` | Workspace path containing silver SQL files |

# COMMAND ----------

dbutils.widgets.text("source_name", "", "Source Name (empty = all)")
dbutils.widgets.text(
    "config_root",
    "/Workspace/Shared/health_platform/config/sources",
    "Config Root",
)
dbutils.widgets.text(
    "sql_root",
    "/Workspace/Shared/health_platform/notebooks/silver/sql",
    "SQL Root",
)

source_name = dbutils.widgets.get("source_name").strip()
config_root = dbutils.widgets.get("config_root").strip()
sql_root    = dbutils.widgets.get("sql_root").strip()

# COMMAND ----------

import glob as _glob
import yaml


def load_source_configs(config_root: str, source_name: str = "") -> list:
    """
    Return a list of source config dicts.

    If source_name is provided, loads only that config.
    Otherwise, loads all *.yml files in config_root that contain a 'silver' block.
    """
    if source_name:
        paths = [f"{config_root}/{source_name}.yml"]
    else:
        paths = sorted(_glob.glob(f"{config_root}/*.yml"))

    configs = []
    for path in paths:
        with open(path) as f:
            cfg = yaml.safe_load(f)
        if "silver" in cfg:
            configs.append(cfg)
        else:
            print(f"Skipping {path} (no 'silver' block found)")

    return configs


def run_silver_for_source(config: dict, sql_root: str) -> None:
    """
    Load and execute the silver SQL for one source config.

    Template variables substituted into the SQL file:
        {source_system}  — source identifier, e.g. apple_health
        {bronze_table}   — fully-qualified bronze table
        {silver_table}   — fully-qualified silver table

    The SQL file controls the operation type (MERGE INTO or INSERT INTO REPLACE WHERE).
    The runner executes every semicolon-delimited statement in the file.
    """
    name          = config["name"]
    source_system = config["source_system"]
    silver_cfg    = config["silver"]
    sql_file      = silver_cfg["sql_file"]
    target_table  = silver_cfg["target_table"]
    bronze_table  = config["bronze"]["autoloader"]["target_table"]

    sql_path = f"{sql_root}/{sql_file}"

    print(f"\n{'=' * 60}")
    print(f"Source:       {name}")
    print(f"Source system:{source_system}")
    print(f"Bronze:       {bronze_table}")
    print(f"Silver:       {target_table}")
    print(f"SQL file:     {sql_file}")
    print(f"Load mode:    {silver_cfg.get('load_mode', 'merge')}")
    print(f"{'=' * 60}\n")

    with open(sql_path) as f:
        sql_template = f.read()

    # Substitute only the known runner variables.
    # SQL files must use {source_system}, {bronze_table}, {silver_table}.
    # Any other curly braces in SQL (e.g. struct literals) must use double braces: {{ }}.
    sql = sql_template.format(
        source_system=source_system,
        bronze_table=bronze_table,
        silver_table=target_table,
    )

    # Execute each statement separated by semicolons.
    # Empty strings (e.g. trailing semicolon) are skipped.
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    for i, statement in enumerate(statements, 1):
        print(f"[{name}] Executing statement {i}/{len(statements)}…")
        spark.sql(statement)

    print(f"[{name}] Done.")


# COMMAND ----------

configs = load_source_configs(config_root, source_name)

if not configs:
    label = f"source '{source_name}'" if source_name else f"config_root '{config_root}'"
    raise ValueError(f"No silver-enabled configs found for {label}.")

print(f"Sources to process: {[c['name'] for c in configs]}\n")

for cfg in configs:
    run_silver_for_source(cfg, sql_root)

print(f"\nAll done. Processed {len(configs)} source(s).")
