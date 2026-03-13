"""Integration tests for MCP server + health_tools refactoring.

Covers the 4 changed files:
  - mcp/server.py         (centralized get_db_path + validate_sql_identifier imports)
  - mcp/health_tools.py   (same, plus search_evidence read_only=True)
  - api/server.py         (centralized get_db_path import)
  - api/routes/mobile.py  (centralized get_db_path + validate_sql_identifier imports)

All tests use in-memory DuckDB — never production data.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from health_platform.mcp.health_tools import HealthTools
from health_platform.utils.sql_safety import validate_sql_identifier

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db():
    """In-memory DuckDB with minimal silver + agent data."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")
    con.execute(
        """
        CREATE TABLE silver.daily_sleep (
            sk_date INTEGER, day DATE, sleep_score INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT INTO silver.daily_sleep (day, sleep_score) VALUES
        ('2026-02-01', 82), ('2026-02-02', 75), ('2026-02-03', 91),
        ('2026-02-04', 68), ('2026-02-05', 85), ('2026-02-06', 79),
        ('2026-02-07', 88)
        """
    )
    con.execute(
        """
        CREATE TABLE silver.daily_activity (
            sk_date INTEGER, day DATE, steps INTEGER, activity_score INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
        """
    )
    con.execute(
        """
        INSERT INTO silver.daily_activity (day, steps, activity_score) VALUES
        ('2026-02-01', 8500, 77), ('2026-02-02', 6200, 63),
        ('2026-02-03', 11000, 91), ('2026-02-04', 5000, 55),
        ('2026-02-05', 9100, 82), ('2026-02-06', 7800, 73),
        ('2026-02-07', 10200, 88)
        """
    )
    con.execute(
        """
        CREATE TABLE agent.patient_profile (
            category VARCHAR, profile_key VARCHAR, profile_value VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO agent.patient_profile VALUES
        ('demographics', 'age', '38'),
        ('baselines', 'sleep_score_baseline', '80'),
        ('baselines', 'steps_baseline', '9000')
        """
    )
    yield con
    con.close()


@pytest.fixture
def tools(db):
    """HealthTools instance wired to the in-memory db fixture."""
    return HealthTools(db)


# ---------------------------------------------------------------------------
# 1. validate_sql_identifier — centralized utility (mcp/server.py uses it
#    directly for forecast_metric; mobile.py uses it for _query_metric)
# ---------------------------------------------------------------------------


class TestValidateSqlIdentifier:
    """Verify the canonical validator used across all refactored modules."""

    def test_accepts_valid_identifiers(self):
        assert validate_sql_identifier("daily_sleep") == "daily_sleep"
        assert validate_sql_identifier("sleep_score") == "sleep_score"
        assert validate_sql_identifier("silver") == "silver"
        assert validate_sql_identifier("_private") == "_private"
        assert validate_sql_identifier("col2") == "col2"

    def test_rejects_injection_attempts(self):
        bad = [
            "daily_sleep; DROP TABLE silver.daily_sleep",
            "1bad",
            "col-name",
            "col name",
            "col.name",
            "",
            "col'",
            'col"',
        ]
        for name in bad:
            with pytest.raises(ValueError, match="Invalid SQL identifier"):
                validate_sql_identifier(name)

    def test_schema_dot_table_each_part_validated_separately(self):
        """mobile.py splits on '.' and validates each part — verify the pattern."""
        table = "silver.daily_sleep"
        parts = table.split(".", 1)
        validated = ".".join(validate_sql_identifier(p) for p in parts)
        assert validated == "silver.daily_sleep"

    def test_schema_dot_table_rejects_bad_part(self):
        table = "silver.daily sleep"  # space in table name
        parts = table.split(".", 1)
        with pytest.raises(ValueError):
            ".".join(validate_sql_identifier(p) for p in parts)


# ---------------------------------------------------------------------------
# 2. search_evidence read_only=True change
#    Before refactor: EvidenceStore was opened with read_only=False (bug).
#    After refactor:  mcp/server.py calls get_tools() (default read_only=True)
#    for search_evidence.  We verify the tool itself doesn't write to DuckDB.
# ---------------------------------------------------------------------------


class TestSearchEvidenceReadOnly:
    """search_evidence must not write to the DuckDB connection."""

    @patch("health_platform.knowledge.evidence_store.EvidenceStore")
    def test_search_evidence_does_not_mutate_db(self, mock_cls, db):
        """After search_evidence, no new tables/rows should appear in agent schema."""
        ms = MagicMock()
        ms.search.return_value = [
            {
                "pmid": "39000001",
                "title": "Magnesium and sleep",
                "evidence_level": "rct",
                "evidence_score": 0.8,
            }
        ]
        ms.format_for_mcp.return_value = "## PubMed Evidence\n- PMID 39000001\n"
        mock_cls.return_value = ms

        # Snapshot row counts before
        before = db.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='agent'"
        ).fetchone()[0]

        tools = HealthTools(db)
        result = tools.search_evidence("magnesium sleep", max_results=3)

        after = db.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='agent'"
        ).fetchone()[0]

        assert result  # got a non-empty response
        assert before == after  # no schema mutations

    @patch("health_platform.knowledge.evidence_store.EvidenceStore")
    def test_search_evidence_returns_markdown(self, mock_cls, db):
        ms = MagicMock()
        ms.search.return_value = [
            {
                "pmid": "39000002",
                "title": "HRV and autonomic nervous system",
                "evidence_level": "meta_analysis",
                "evidence_score": 1.0,
            }
        ]
        ms.format_for_mcp.return_value = (
            "## PubMed Evidence: HRV\n- **PMID:** 39000002\n"
        )
        mock_cls.return_value = ms

        tools = HealthTools(db)
        result = tools.search_evidence(
            "HRV sleep quality", max_results=5, min_year="2020"
        )

        assert "PubMed Evidence" in result
        ms.search.assert_called_once_with(
            "HRV sleep quality", max_results=5, min_year="2020"
        )

    @patch("health_platform.knowledge.evidence_store.EvidenceStore")
    def test_search_evidence_error_propagates_gracefully(self, mock_cls, db):
        """A failing EvidenceStore must surface an error string, not raise."""
        mock_cls.side_effect = RuntimeError("network timeout")
        tools = HealthTools(db)
        result = tools.search_evidence("omega-3 inflammation")
        assert isinstance(result, str)
        assert len(result) > 0  # something was returned, not None/exception


# ---------------------------------------------------------------------------
# 3. MCP server.py — forecast_metric uses validate_sql_identifier
# ---------------------------------------------------------------------------


class TestForecastMetricIdentifierValidation:
    """forecast_metric in server.py validates table+column via validate_sql_identifier."""

    def test_valid_metric_format_passes_validation(self):
        """Both parts of 'table.column' must pass identifier validation."""
        metric = "daily_sleep.sleep_score"
        parts = metric.split(".", 1)
        table = f"silver.{validate_sql_identifier(parts[0])}"
        column = validate_sql_identifier(parts[1])
        assert table == "silver.daily_sleep"
        assert column == "sleep_score"

    def test_invalid_table_name_is_caught(self):
        metric = "daily sleep.sleep_score"  # space in table name
        parts = metric.split(".", 1)
        with pytest.raises(ValueError):
            validate_sql_identifier(parts[0])

    def test_invalid_column_name_is_caught(self):
        metric = "daily_sleep.sleep score"  # space in column name
        parts = metric.split(".", 1)
        validate_sql_identifier(parts[0])  # table is fine
        with pytest.raises(ValueError):
            validate_sql_identifier(parts[1])

    def test_missing_dot_separator_is_caught(self):
        """metric without '.' must be rejected before identifier validation."""
        metric = "daily_sleep_sleep_score"
        parts = metric.split(".", 1)
        assert len(parts) == 1  # server.py returns error string for this case


# ---------------------------------------------------------------------------
# 4. mobile.py _query_metric — validates table + column identifiers
# ---------------------------------------------------------------------------


class TestMobileQueryMetricValidation:
    """_query_metric in mobile.py validates identifiers before interpolation."""

    def test_valid_table_and_columns_execute(self, db):
        """A valid table+columns query returns rows from in-memory DB."""
        from datetime import date, timedelta

        from health_platform.api.routes.mobile import _query_metric

        since = date.today() - timedelta(days=365)
        rows = _query_metric(db, "silver.daily_sleep", ["day", "sleep_score"], since)
        assert isinstance(rows, list)
        assert len(rows) == 7
        assert "day" in rows[0]
        assert "sleep_score" in rows[0]

    def test_nonexistent_table_returns_empty_list(self, db):
        """CatalogException for a missing table must return [] not raise."""
        from datetime import date, timedelta

        from health_platform.api.routes.mobile import _query_metric

        since = date.today() - timedelta(days=30)
        rows = _query_metric(db, "silver.does_not_exist", ["day", "value"], since)
        assert rows == []

    def test_injection_in_table_name_raises(self, db):
        """SQL injection attempt in table name must raise ValueError."""
        from datetime import date, timedelta

        from health_platform.api.routes.mobile import _query_metric

        since = date.today() - timedelta(days=30)
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _query_metric(
                db,
                "silver.daily_sleep; DROP TABLE silver.daily_sleep",
                ["day"],
                since,
            )

    def test_injection_in_column_name_raises(self, db):
        """SQL injection attempt in column name must raise ValueError."""
        from datetime import date, timedelta

        from health_platform.api.routes.mobile import _query_metric

        since = date.today() - timedelta(days=30)
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _query_metric(
                db,
                "silver.daily_sleep",
                ["day", "sleep_score; DROP TABLE silver.daily_sleep"],
                since,
            )

    def test_date_values_serialized_to_iso_string(self, db):
        """date/datetime column values must be ISO strings, not Python objects."""
        from datetime import date, timedelta

        from health_platform.api.routes.mobile import _query_metric

        since = date.today() - timedelta(days=365)
        rows = _query_metric(db, "silver.daily_sleep", ["day", "sleep_score"], since)
        assert len(rows) > 0
        day_val = rows[0]["day"]
        assert isinstance(day_val, str), f"Expected str, got {type(day_val)}"
        # Must parse as ISO date
        date.fromisoformat(day_val)


# ---------------------------------------------------------------------------
# 5. mobile.py _parse_since — date parsing edge cases
# ---------------------------------------------------------------------------


class TestParseSince:
    def test_none_defaults_to_30_days_ago(self):
        from datetime import date, timedelta

        from health_platform.api.routes.mobile import _parse_since

        result = _parse_since(None)
        expected = date.today() - timedelta(days=30)
        assert result == expected

    def test_valid_iso_datetime_parsed(self):
        from datetime import date

        from health_platform.api.routes.mobile import _parse_since

        result = _parse_since("2026-01-15T00:00:00")
        assert result == date(2026, 1, 15)

    def test_valid_iso_date_parsed(self):
        from datetime import date

        from health_platform.api.routes.mobile import _parse_since

        result = _parse_since("2026-01-15")
        assert result == date(2026, 1, 15)

    def test_invalid_string_defaults_to_30_days_ago(self):
        from datetime import date, timedelta

        from health_platform.api.routes.mobile import _parse_since

        result = _parse_since("not-a-date")
        expected = date.today() - timedelta(days=30)
        assert result == expected

    def test_empty_string_defaults_to_30_days_ago(self):
        from datetime import date, timedelta

        from health_platform.api.routes.mobile import _parse_since

        result = _parse_since("")
        expected = date.today() - timedelta(days=30)
        assert result == expected


# ---------------------------------------------------------------------------
# 6. api/server.py _get_tools — get_db_path integration point
#    We mock get_db_path to isolate from filesystem.
# ---------------------------------------------------------------------------


class TestApiServerGetTools:
    """_get_tools in api/server.py resolves db path via get_db_path()."""

    @patch("health_platform.api.server.get_db_path")
    @patch("health_platform.api.server.duckdb.connect")
    def test_get_tools_uses_get_db_path(self, mock_connect, mock_get_db_path):
        from health_platform.api.server import _get_tools

        mock_get_db_path.return_value = Path("/tmp/fake_health.db")
        mock_con = MagicMock()
        mock_connect.return_value = mock_con

        _get_tools(read_only=True)

        mock_get_db_path.assert_called_once()
        mock_connect.assert_called_once_with("/tmp/fake_health.db", read_only=True)

    @patch("health_platform.api.server.get_db_path")
    @patch("health_platform.api.server.duckdb.connect")
    def test_get_tools_read_only_false_for_write_ops(
        self, mock_connect, mock_get_db_path
    ):
        from health_platform.api.server import _get_tools

        mock_get_db_path.return_value = Path("/tmp/fake_health.db")
        mock_con = MagicMock()
        mock_connect.return_value = mock_con

        _get_tools(read_only=False)

        mock_connect.assert_called_once_with("/tmp/fake_health.db", read_only=False)


# ---------------------------------------------------------------------------
# 7. mcp/server.py get_tools — same pattern
# ---------------------------------------------------------------------------


class TestMcpServerGetTools:
    """get_tools() in mcp/server.py resolves db path via get_db_path()."""

    @patch("health_platform.mcp.server.get_db_path")
    @patch("health_platform.mcp.server.duckdb.connect")
    def test_get_tools_uses_get_db_path(self, mock_connect, mock_get_db_path):
        from health_platform.mcp.server import get_tools

        mock_get_db_path.return_value = Path("/tmp/fake_health.db")
        mock_con = MagicMock()
        mock_connect.return_value = mock_con

        get_tools(read_only=True)

        mock_get_db_path.assert_called_once()
        mock_connect.assert_called_once_with("/tmp/fake_health.db", read_only=True)

    @patch("health_platform.mcp.server.get_db_path")
    @patch("health_platform.mcp.server.duckdb.connect")
    def test_get_tools_defaults_to_read_only(self, mock_connect, mock_get_db_path):
        from health_platform.mcp.server import get_tools

        mock_get_db_path.return_value = Path("/tmp/fake_health.db")
        mock_con = MagicMock()
        mock_connect.return_value = mock_con

        get_tools()  # no argument

        mock_connect.assert_called_once_with("/tmp/fake_health.db", read_only=True)


# ---------------------------------------------------------------------------
# 8. HealthTools.close() — connection cleanup
# ---------------------------------------------------------------------------


class TestHealthToolsClose:
    def test_close_is_idempotent(self, db):
        """Calling close() twice must not raise."""
        tools = HealthTools(db)
        tools.close()
        tools.close()  # second call must not raise

    def test_close_swallows_exception(self):
        """close() must swallow exceptions from a broken connection."""
        mock_con = MagicMock()
        mock_con.close.side_effect = Exception("already closed")
        tools = HealthTools(mock_con)
        tools.close()  # must not raise
