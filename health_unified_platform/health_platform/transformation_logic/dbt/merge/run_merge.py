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


def load_config() -> dict:
    """Load environment_config.yaml from the standard config location."""
    config_path = (
        Path(__file__).resolve().parents[4]
        / "health_environment"
        / "config"
        / "environment_config.yaml"
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


def main():
    parser = argparse.ArgumentParser(description="Run a merge SQL script against DuckDB")
    parser.add_argument("sql_file", help="Path to the merge SQL file (relative to merge/ dir)")
    args = parser.parse_args()

    merge_dir = Path(__file__).resolve().parent
    sql_path = merge_dir / args.sql_file
    if not sql_path.exists():
        sys.exit(f"SQL file not found: {sql_path}")

    config = load_config()
    db_path = get_db_path(config)
    print(f"Database: {db_path}")
    print(f"SQL file: {sql_path.name}")

    sql = sql_path.read_text()
    statements = split_statements(sql)

    con = duckdb.connect(db_path)
    try:
        for i, stmt in enumerate(statements, 1):
            first_line = stmt.lstrip().split("\n")[0][:80]
            print(f"\n[{i}/{len(statements)}] {first_line}...")
            result = con.execute(stmt)
            if result and result.description:
                rows = result.fetchall()
                print(f"  -> {len(rows)} rows returned")
    finally:
        con.close()

    print("\nDone.")


if __name__ == "__main__":
    main()
