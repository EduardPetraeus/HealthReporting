import glob
import os
import sys
from pathlib import Path

import duckdb
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from health_platform.utils.audit_logger import AuditLogger
from health_platform.utils.logging_config import get_logger
from health_platform.utils.path_resolver import get_project_root

logger = get_logger("ingestion_engine")


def load_yaml(path):
    """loads_a_yaml_configuration_file"""
    with open(path, "r") as f:
        return yaml.safe_load(f)


def run_ingestion():
    # load_environment_and_source_configurations
    root = get_project_root()
    env_path = root / "health_environment" / "config" / "environment_config.yaml"
    src_path = root / "health_environment" / "config" / "sources_config.yaml"

    env_cfg = load_yaml(env_path)
    src_cfg = load_yaml(src_path)

    # setup_environment_and_database_paths
    active_env = os.getenv("HEALTH_ENV", env_cfg["defaults"]["environment"])
    db_name = env_cfg["defaults"]["database_name"]

    db_file = Path(env_cfg["paths"]["db_root"]) / f"{db_name}_{active_env}.db"
    lake_root = Path(env_cfg["paths"]["data_lake_root"])

    logger.info(f"Starting ingestion engine [env: {active_env}]")
    logger.info(f"Target database: {db_file}")

    with AuditLogger("ingestion_engine", "bronze", "multi_source") as audit:
        # establish_connection_to_duckdb
        con = duckdb.connect(str(db_file))

        for source in src_cfg["sources"]:
            name = source["name"]
            schema = source["target_schema"]
            table = source["target_table"]
            input_glob = str(lake_root / source["relative_path"])

            logger.info(f"Processing source: {name} -> {schema}.{table}")
            logger.debug(f"Searching path: {input_glob}")

            # verify_file_existence
            found_files = glob.glob(input_glob, recursive=True)
            logger.debug(f"Found {len(found_files)} files matching the pattern")

            if not found_files:
                logger.warning(f"No files found at path: {input_glob}")
                audit.log_table(
                    f"{schema}.{table}",
                    "CREATE_OR_REPLACE",
                    status="error",
                    error_message="no_files_found",
                )
                continue

            # ensure_target_schema_exists
            # WARN: schema and table are f-string interpolated into SQL.
            # Safe only because both values come from trusted YAML config files.
            # Never pass user-supplied input into these variables.
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            # ingestion_logic_with_union_by_name_for_schema_evolution
            query = f"""
                CREATE OR REPLACE TABLE {schema}.{table} AS
                SELECT
                    *,
                    current_timestamp as _ingested_at,
                    '{active_env}' as _source_env
                FROM read_parquet('{input_glob}', hive_partitioning=True, union_by_name=True)
            """

            try:
                con.execute(query)
                count = con.execute(
                    f"SELECT count(*) FROM {schema}.{table}"
                ).fetchone()[0]
                logger.info(f"Success: {count:,} rows ingested -> {schema}.{table}")
                audit.log_table(
                    f"{schema}.{table}",
                    "CREATE_OR_REPLACE",
                    rows_after=count,
                    status="success",
                )
            except Exception as e:
                logger.error(f"Error during ingestion of {name}: {e}")
                audit.log_table(
                    f"{schema}.{table}",
                    "CREATE_OR_REPLACE",
                    status="error",
                    error_message=str(e),
                )

        con.close()

    # Post-ingestion: trigger daily summary generation for today
    try:
        from datetime import date

        from health_platform.ai.text_generator import generate_summary_for_pipeline

        con = duckdb.connect(str(db_file))
        summary = generate_summary_for_pipeline(con, date.today())
        if summary:
            logger.info(f"Daily summary generated ({len(summary)} chars)")
        con.close()
    except Exception as e:
        logger.warning(f"Daily summary generation skipped: {e}")

    logger.info("Ingestion complete")


# this_part_is_critical_to_actually_run_the_script
if __name__ == "__main__":
    run_ingestion()
