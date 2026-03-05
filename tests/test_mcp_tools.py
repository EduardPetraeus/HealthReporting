"""Tests for MCP health tools."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "health_unified_platform"),
)


CONTRACTS_DIR = (
    Path(__file__).resolve().parents[1]
    / "health_unified_platform"
    / "health_platform"
    / "contracts"
    / "metrics"
)


class TestQueryBuilder:
    """Test YAML contract query builder."""

    def test_load_contract(self):
        """Load a metric contract from YAML."""
        if not (CONTRACTS_DIR / "sleep_score.yml").exists():
            pytest.skip("sleep_score.yml not yet created")

        from health_platform.mcp.query_builder import QueryBuilder

        qb = QueryBuilder(CONTRACTS_DIR)
        contract = qb.load_contract("sleep_score")

        assert contract is not None
        metric = contract.get("metric", contract)
        assert metric["name"] == "sleep_score"

    def test_list_metrics(self):
        """List available metrics."""
        if not CONTRACTS_DIR.exists():
            pytest.skip("Contracts directory not populated")

        from health_platform.mcp.query_builder import QueryBuilder

        qb = QueryBuilder(CONTRACTS_DIR)
        metrics = qb.list_metrics()

        assert isinstance(metrics, list)

    def test_get_index(self):
        """Load master index."""
        if not (CONTRACTS_DIR / "_index.yml").exists():
            pytest.skip("_index.yml not yet created")

        from health_platform.mcp.query_builder import QueryBuilder

        qb = QueryBuilder(CONTRACTS_DIR)
        index = qb.get_index()

        assert "metrics" in index

    def test_build_query(self):
        """Build parameterized SQL from contract."""
        if not (CONTRACTS_DIR / "sleep_score.yml").exists():
            pytest.skip("sleep_score.yml not yet created")

        from health_platform.mcp.query_builder import QueryBuilder

        qb = QueryBuilder(CONTRACTS_DIR)
        sql = qb.build_query(
            "sleep_score",
            "daily_value",
            {"start": "2026-03-01", "end": "2026-03-01"},
        )

        assert sql is not None
        assert "2026-03-01" in sql
        assert "SELECT" in sql.upper()
        assert "BETWEEN" in sql.upper()


class TestFormatter:
    """Test output formatting."""

    def test_markdown_table(self):
        """Format results as markdown table."""
        from health_platform.mcp.formatter import format_as_markdown_table

        rows = [
            ("2026-03-01", 82),
            ("2026-03-02", 75),
        ]
        columns = ["day", "sleep_score"]
        result = format_as_markdown_table(rows, columns)

        assert "| day" in result
        assert "| 2026-03-01 |" in result
        assert "---" in result  # separator row

    def test_markdown_kv(self):
        """Format dict as markdown key-value."""
        from health_platform.mcp.formatter import format_as_markdown_kv

        data = {"name": "John", "age": 45}
        result = format_as_markdown_kv(data)

        assert "name" in result
        assert "45" in result

    def test_format_error(self):
        """Error formatting includes message."""
        from health_platform.mcp.formatter import format_error

        result = format_error("Something went wrong")
        assert "Something went wrong" in result

    def test_format_empty(self):
        """Empty result formatting."""
        from health_platform.mcp.formatter import format_empty

        result = format_empty("sleep_score", "last_7_days")
        assert "sleep_score" in result or "no data" in result.lower()


class TestSchemaPruner:
    """Test schema context pruning."""

    def test_get_relevant_schema(self, seeded_db):
        """Get schema context for a category."""
        if not (CONTRACTS_DIR / "_index.yml").exists():
            pytest.skip("_index.yml not yet created")

        from health_platform.mcp.schema_pruner import SchemaPruner

        pruner = SchemaPruner(seeded_db, CONTRACTS_DIR)
        context = pruner.get_relevant_schema("sleep")

        assert isinstance(context, str)
        assert "daily_sleep" in context


class TestHealthToolsIntegration:
    """Integration tests for health tools."""

    def _setup_full_db(self, seeded_db):
        """Set up all required tables for integration testing."""
        # Agent tables
        seeded_db.execute("""
            CREATE TABLE IF NOT EXISTS agent.patient_profile (
                profile_key VARCHAR PRIMARY KEY,
                profile_value VARCHAR NOT NULL,
                numeric_value DOUBLE,
                category VARCHAR NOT NULL,
                description VARCHAR NOT NULL,
                computed_from VARCHAR,
                last_updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                update_frequency VARCHAR
            )
        """)
        seeded_db.execute("""
            CREATE TABLE IF NOT EXISTS agent.daily_summaries (
                day DATE PRIMARY KEY,
                sleep_score INTEGER, readiness_score INTEGER,
                steps INTEGER, resting_hr DOUBLE,
                stress_level VARCHAR,
                has_anomaly BOOLEAN DEFAULT FALSE,
                anomaly_metrics VARCHAR,
                summary_text VARCHAR NOT NULL,
                embedding FLOAT[384],
                data_completeness DOUBLE,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)
        seeded_db.execute("""
            CREATE TABLE IF NOT EXISTS agent.knowledge_base (
                insight_id VARCHAR PRIMARY KEY,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                insight_type VARCHAR NOT NULL,
                title VARCHAR NOT NULL,
                content VARCHAR NOT NULL,
                evidence_query VARCHAR,
                confidence DOUBLE,
                tags VARCHAR[],
                embedding FLOAT[384],
                is_active BOOLEAN DEFAULT TRUE,
                superseded_by VARCHAR
            )
        """)
        return seeded_db

    def test_query_health_daily(self, seeded_db):
        """Query health metric for a specific day."""
        if not (CONTRACTS_DIR / "sleep_score.yml").exists():
            pytest.skip("Contracts not yet created")

        db = self._setup_full_db(seeded_db)

        from health_platform.mcp.health_tools import HealthTools

        tools = HealthTools(db)
        result = tools.query_health("sleep_score", "2026-03-01:2026-03-01")

        assert result is not None
        assert isinstance(result, str)

    def test_get_profile(self, seeded_db):
        """Get patient profile."""
        db = self._setup_full_db(seeded_db)

        # Seed some profile data
        db.execute("""
            INSERT INTO agent.patient_profile
            (profile_key, profile_value, numeric_value, category, description)
            VALUES ('age', '45', 45, 'demographics', 'Patient age')
        """)

        from health_platform.mcp.health_tools import HealthTools

        tools = HealthTools(db)
        result = tools.get_profile()

        assert "45" in result

    def test_run_custom_query_readonly(self, seeded_db):
        """Custom query rejects write operations."""
        db = self._setup_full_db(seeded_db)

        from health_platform.mcp.health_tools import HealthTools

        tools = HealthTools(db)
        result = tools.run_custom_query(
            "DROP TABLE silver.daily_sleep",
            "testing write rejection",
        )

        assert "error" in result.lower() or "denied" in result.lower() or "not allowed" in result.lower()

    def test_run_custom_query_select(self, seeded_db):
        """Custom query allows SELECT."""
        db = self._setup_full_db(seeded_db)

        from health_platform.mcp.health_tools import HealthTools

        tools = HealthTools(db)
        result = tools.run_custom_query(
            "SELECT COUNT(*) AS cnt FROM silver.daily_sleep",
            "counting sleep records",
        )

        assert "3" in result or "cnt" in result
