"""
run_merge.py — Execute a per-source merge SQL script against DuckDB.

Usage:
    python run_merge.py silver/merge_apple_health_heart_rate.sql
    HEALTH_ENV=prd python run_merge.py silver/merge_apple_health_heart_rate.sql
"""

import argparse
import os
import sys
from pathlib import Path

import duckdb
import yaml
from health_platform.utils.audit_logger import AuditLogger
from health_platform.utils.logging_config import get_logger
from health_platform.utils.path_resolver import get_project_root

logger = get_logger("run_merge")


def load_config() -> dict:
    """Load environment_config.yaml from the standard config location."""
    config_path = (
        get_project_root() / "health_environment" / "config" / "environment_config.yaml"
    )
    if not config_path.exists():
        sys.exit(f"Config not found: {config_path}")
    with open(config_path) as f:
        return yaml.safe_load(f)


def get_db_path(config: dict) -> str:
    """Resolve the DuckDB file path based on HEALTH_ENV."""
    env = os.environ.get("HEALTH_ENV", config["defaults"]["environment"])
    db_name = config["defaults"]["database_name"]
    db_root = config["paths"]["db_root"]
    return os.path.join(db_root, f"{db_name}_{env}.db")


def split_statements(sql: str) -> list[str]:
    """Split a SQL file into individual statements on semicolons."""
    statements = []
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            statements.append(stmt)
    return statements


def _source_system_from_path(sql_file_name: str) -> str:
    """Extract source system from merge file name, e.g. merge_oura_heartrate -> oura."""
    parts = sql_file_name.replace(".sql", "").split("_")
    if len(parts) >= 2:
        return parts[1]
    return "unknown"


def main():
    parser = argparse.ArgumentParser(
        description="Run a merge SQL script against DuckDB"
    )
    parser.add_argument(
        "sql_file", help="Path to the merge SQL file (relative to merge/ dir)"
    )
    args = parser.parse_args()

    merge_dir = Path(__file__).resolve().parent
    sql_path = merge_dir / args.sql_file
    if not sql_path.exists():
        sys.exit(f"SQL file not found: {sql_path}")

    config = load_config()
    db_path = get_db_path(config)
    source_system = _source_system_from_path(sql_path.name)

    logger.info(f"run_merge starting: {sql_path.name}")
    logger.info(f"Database: {db_path}")

    sql = sql_path.read_text()
    statements = split_statements(sql)

    with AuditLogger("run_merge", "silver", source_system) as audit:
        con = duckdb.connect(db_path)
        try:
            for i, stmt in enumerate(statements, 1):
                first_line = stmt.lstrip().split("\n")[0][:80]
                logger.info(f"[{i}/{len(statements)}] {first_line}...")
                result = con.execute(stmt)
                if result and result.description:
                    rows = result.fetchall()
                    logger.debug(f"  -> {len(rows)} rows returned")
        finally:
            con.close()

        audit.log_table(sql_path.stem, "MERGE", status="success")

    logger.info("Done.")


if __name__ == "__main__":
    main()
