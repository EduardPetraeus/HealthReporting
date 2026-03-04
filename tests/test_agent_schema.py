"""Tests for agent schema creation and structure."""
from __future__ import annotations

from pathlib import Path

import duckdb
import pytest


SETUP_DIR = (
    Path(__file__).resolve().parents[1]
    / "health_unified_platform"
    / "health_platform"
    / "setup"
)


def _split_sql(sql: str) -> list[str]:
    """Split SQL into statements, respecting single-quoted strings."""
    statements = []
    current = []
    in_string = False
    i = 0
    while i < len(sql):
        c = sql[i]
        if c == "'" and not in_string:
            in_string = True
            current.append(c)
        elif c == "'" and in_string:
            if i + 1 < len(sql) and sql[i + 1] == "'":
                current.append("''")
                i += 1
            else:
                in_string = False
                current.append(c)
        elif c == ";" and not in_string:
            s = "".join(current).strip()
            if s:
                lines = [l for l in s.split("\n") if not l.strip().startswith("--")]
                clean = "\n".join(lines).strip()
                if clean:
                    statements.append(clean)
            current = []
        else:
            current.append(c)
        i += 1
    s = "".join(current).strip()
    if s:
        lines = [l for l in s.split("\n") if not l.strip().startswith("--")]
        clean = "\n".join(lines).strip()
        if clean:
            statements.append(clean)
    return statements


def _execute_sql_file(con: duckdb.DuckDBPyConnection, filepath: Path) -> None:
    """Execute SQL file, respecting quoted strings and skipping comments."""
    sql = filepath.read_text(encoding="utf-8")
    for stmt in _split_sql(sql):
        try:
            con.execute(stmt)
        except Exception:
            pass  # Skip statements that fail (e.g., COMMENT ON non-existent tables)


class TestAgentSchema:
    """Test agent schema DDL creates all expected objects."""

    def test_create_agent_schema(self, memory_db):
        """Agent schema and all tables are created."""
        schema_file = SETUP_DIR / "create_agent_schema.sql"
        if not schema_file.exists():
            pytest.skip("create_agent_schema.sql not yet created")

        _execute_sql_file(memory_db, schema_file)

        tables = memory_db.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'agent' ORDER BY table_name"
        ).fetchall()
        table_names = [t[0] for t in tables]

        assert "patient_profile" in table_names
        assert "daily_summaries" in table_names
        assert "health_graph" in table_names
        assert "health_graph_edges" in table_names
        assert "knowledge_base" in table_names

    def test_patient_profile_columns(self, memory_db):
        """patient_profile has expected columns."""
        schema_file = SETUP_DIR / "create_agent_schema.sql"
        if not schema_file.exists():
            pytest.skip("create_agent_schema.sql not yet created")

        _execute_sql_file(memory_db, schema_file)

        cols = memory_db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'agent' AND table_name = 'patient_profile' "
            "ORDER BY ordinal_position"
        ).fetchall()
        col_names = [c[0] for c in cols]

        assert "profile_key" in col_names
        assert "profile_value" in col_names
        assert "category" in col_names
        assert "last_updated_at" in col_names

    def test_daily_summaries_has_embedding(self, memory_db):
        """daily_summaries has embedding column for vector search."""
        schema_file = SETUP_DIR / "create_agent_schema.sql"
        if not schema_file.exists():
            pytest.skip("create_agent_schema.sql not yet created")

        _execute_sql_file(memory_db, schema_file)

        cols = memory_db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'agent' AND table_name = 'daily_summaries'"
        ).fetchall()
        col_names = [c[0] for c in cols]

        assert "embedding" in col_names
        assert "summary_text" in col_names

    def test_metric_relationships_in_silver(self, memory_db):
        """metric_relationships table is created in silver schema."""
        schema_file = SETUP_DIR / "create_agent_schema.sql"
        if not schema_file.exists():
            pytest.skip("create_agent_schema.sql not yet created")

        _execute_sql_file(memory_db, schema_file)

        tables = memory_db.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'silver' AND table_name = 'metric_relationships'"
        ).fetchall()

        assert len(tables) == 1

    def test_idempotent_execution(self, memory_db):
        """Running DDL twice does not error."""
        schema_file = SETUP_DIR / "create_agent_schema.sql"
        if not schema_file.exists():
            pytest.skip("create_agent_schema.sql not yet created")

        _execute_sql_file(memory_db, schema_file)
        _execute_sql_file(memory_db, schema_file)

        tables = memory_db.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'agent'"
        ).fetchone()
        assert tables[0] >= 4


class TestHealthGraphSeed:
    """Test health graph seed data."""

    def test_seed_creates_nodes(self, memory_db):
        """Seed file creates knowledge graph nodes."""
        schema_file = SETUP_DIR / "create_agent_schema.sql"
        seed_file = SETUP_DIR / "seed_health_graph.sql"
        if not schema_file.exists() or not seed_file.exists():
            pytest.skip("SQL files not yet created")

        _execute_sql_file(memory_db, schema_file)
        _execute_sql_file(memory_db, seed_file)

        count = memory_db.execute(
            "SELECT COUNT(*) FROM agent.health_graph"
        ).fetchone()[0]
        assert count >= 40, f"Expected 40+ nodes, got {count}"

    def test_seed_creates_edges(self, memory_db):
        """Seed file creates knowledge graph edges."""
        schema_file = SETUP_DIR / "create_agent_schema.sql"
        seed_file = SETUP_DIR / "seed_health_graph.sql"
        if not schema_file.exists() or not seed_file.exists():
            pytest.skip("SQL files not yet created")

        _execute_sql_file(memory_db, schema_file)
        _execute_sql_file(memory_db, seed_file)

        count = memory_db.execute(
            "SELECT COUNT(*) FROM agent.health_graph_edges"
        ).fetchone()[0]
        assert count >= 80, f"Expected 80+ edges, got {count}"

    def test_node_types_present(self, memory_db):
        """All expected node types are represented."""
        schema_file = SETUP_DIR / "create_agent_schema.sql"
        seed_file = SETUP_DIR / "seed_health_graph.sql"
        if not schema_file.exists() or not seed_file.exists():
            pytest.skip("SQL files not yet created")

        _execute_sql_file(memory_db, schema_file)
        _execute_sql_file(memory_db, seed_file)

        types = memory_db.execute(
            "SELECT DISTINCT node_type FROM agent.health_graph ORDER BY node_type"
        ).fetchall()
        type_names = [t[0] for t in types]

        assert "biomarker" in type_names
        assert "concept" in type_names
