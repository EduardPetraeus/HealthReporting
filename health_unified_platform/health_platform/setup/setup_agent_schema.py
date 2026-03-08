"""Setup script for the AI-native agent schema.

Creates the agent schema, tables, and seeds the health knowledge graph
in the local DuckDB database. Idempotent — safe to run multiple times.

Usage:
    HEALTH_ENV=dev python -m health_platform.setup.setup_agent_schema
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from health_platform.utils.logging_config import get_logger
from health_platform.utils.path_resolver import get_project_root

logger = get_logger("setup_agent_schema")

SETUP_DIR = Path(__file__).resolve().parent


def split_sql_statements(sql: str) -> list[str]:
    """Split SQL text into statements, respecting quoted strings.

    Handles semicolons inside single-quoted strings correctly.
    """
    statements = []
    current = []
    in_string = False
    i = 0
    while i < len(sql):
        char = sql[i]
        if char == "'" and not in_string:
            in_string = True
            current.append(char)
        elif char == "'" and in_string:
            # Check for escaped quote ('')
            if i + 1 < len(sql) and sql[i + 1] == "'":
                current.append("''")
                i += 1
            else:
                in_string = False
                current.append(char)
        elif char == ";" and not in_string:
            stmt = "".join(current).strip()
            if stmt:
                # Remove comment-only lines
                lines = [
                    line
                    for line in stmt.split("\n")
                    if not line.strip().startswith("--")
                ]
                clean = "\n".join(lines).strip()
                if clean:
                    statements.append(clean)
            current = []
        else:
            current.append(char)
        i += 1
    # Handle last statement
    stmt = "".join(current).strip()
    if stmt:
        lines = [line for line in stmt.split("\n") if not line.strip().startswith("--")]
        clean = "\n".join(lines).strip()
        if clean:
            statements.append(clean)
    return statements


def execute_sql_file(con: duckdb.DuckDBPyConnection, filepath: Path) -> None:
    """Execute a SQL file against DuckDB, splitting on statement-ending semicolons."""
    sql = filepath.read_text(encoding="utf-8")
    statements = split_sql_statements(sql)
    executed = 0
    for stmt in statements:
        try:
            con.execute(stmt)
            executed += 1
        except Exception as e:
            logger.warning(f"Statement skipped ({filepath.name}): {e}")
    logger.info(f"Executed {executed} statements from {filepath.name}")


def run_setup() -> None:
    """Create agent schema and seed knowledge graph."""
    root = get_project_root()
    env_cfg_path = root / "health_environment" / "config" / "environment_config.yaml"

    import yaml

    with open(env_cfg_path) as f:
        env_cfg = yaml.safe_load(f)

    active_env = os.getenv("HEALTH_ENV", env_cfg["defaults"]["environment"])
    db_name = env_cfg["defaults"]["database_name"]
    db_file = Path(env_cfg["paths"]["db_root"]) / f"{db_name}_{active_env}.db"

    logger.info(f"Setting up agent schema [env: {active_env}]")
    logger.info(f"Target database: {db_file}")

    con = duckdb.connect(str(db_file))

    try:
        # Ensure silver schema exists (for metric_relationships)
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")

        # Create agent schema and tables
        schema_file = SETUP_DIR / "create_agent_schema.sql"
        if schema_file.exists():
            execute_sql_file(con, schema_file)
            logger.info("Agent schema created successfully")
        else:
            logger.error(f"Schema file not found: {schema_file}")
            return

        # Add column comments to silver tables
        comments_file = SETUP_DIR / "add_column_comments.sql"
        if comments_file.exists():
            execute_sql_file(con, comments_file)
            logger.info("Column comments added successfully")
        else:
            logger.warning("Column comments file not found — skipping")

        # Seed health knowledge graph
        seed_file = SETUP_DIR / "seed_health_graph.sql"
        if seed_file.exists():
            # Check if already seeded
            try:
                count = con.execute(
                    "SELECT COUNT(*) FROM agent.health_graph"
                ).fetchone()[0]
                if count > 0:
                    logger.info(
                        f"Health graph already seeded ({count} nodes) — skipping"
                    )
                else:
                    execute_sql_file(con, seed_file)
                    count = con.execute(
                        "SELECT COUNT(*) FROM agent.health_graph"
                    ).fetchone()[0]
                    edge_count = con.execute(
                        "SELECT COUNT(*) FROM agent.health_graph_edges"
                    ).fetchone()[0]
                    logger.info(
                        f"Health graph seeded: {count} nodes, {edge_count} edges"
                    )
            except Exception:
                execute_sql_file(con, seed_file)
                logger.info("Health graph seeded")
        else:
            logger.warning("Seed file not found — skipping")

        # Add evidence columns to health_graph_edges (idempotent for existing DBs)
        for col_stmt in [
            "ALTER TABLE agent.health_graph_edges ADD COLUMN IF NOT EXISTS pmid VARCHAR",
            "ALTER TABLE agent.health_graph_edges ADD COLUMN IF NOT EXISTS doi VARCHAR",
            "ALTER TABLE agent.health_graph_edges ADD COLUMN IF NOT EXISTS citation_text VARCHAR",
        ]:
            try:
                con.execute(col_stmt)
            except Exception:
                pass  # Column already exists or table not yet created

        # Verify setup
        schemas = con.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = 'agent'"
        ).fetchall()
        if schemas:
            tables = con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'agent' ORDER BY table_name"
            ).fetchall()
            logger.info(f"Agent schema verified: {[t[0] for t in tables]}")
        else:
            logger.error("Agent schema not found after setup!")

    finally:
        con.close()

    logger.info("Agent schema setup complete")


if __name__ == "__main__":
    run_setup()
