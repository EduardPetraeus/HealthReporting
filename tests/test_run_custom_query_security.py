"""Security tests for HealthTools.run_custom_query.

Verifies that the keyword filter blocks all forbidden DML/DDL operations
and that valid SELECT queries execute correctly.
"""

from __future__ import annotations

import duckdb
import pytest
from health_platform.mcp.health_tools import HealthTools

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def query_tools():
    """HealthTools with in-memory DuckDB and a test table."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")
    con.execute(
        """
        CREATE TABLE silver.test_data (
            id INTEGER, name VARCHAR, value DOUBLE
        )
        """
    )
    con.execute(
        """
        INSERT INTO silver.test_data VALUES
            (1, 'alpha', 10.5),
            (2, 'beta', 20.3),
            (3, 'gamma', 30.1)
        """
    )
    tools = HealthTools(con)
    yield tools
    con.close()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestRunCustomQueryHappyPath:
    """Valid SELECT queries should succeed and return data."""

    def test_select_succeeds(self, query_tools):
        """A plain SELECT returns rows containing expected data."""
        result = query_tools.run_custom_query(
            "SELECT * FROM silver.test_data",
            "retrieve all test rows",
        )
        assert "alpha" in result

    def test_no_results_message(self, query_tools):
        """SELECT that matches no rows returns the no-results message."""
        result = query_tools.run_custom_query(
            "SELECT * FROM silver.test_data WHERE id > 999",
            "query beyond existing id range",
        )
        assert "no results" in result.lower()


# ---------------------------------------------------------------------------
# Forbidden keyword — DML / DDL
# ---------------------------------------------------------------------------


class TestForbiddenKeywords:
    """All forbidden keywords must be blocked before execution."""

    def test_drop_blocked(self, query_tools):
        """DROP TABLE is rejected with an error."""
        result = query_tools.run_custom_query(
            "DROP TABLE silver.test_data",
            "attempt to drop table",
        )
        assert "Error" in result

    def test_insert_blocked(self, query_tools):
        """INSERT is rejected with an error."""
        result = query_tools.run_custom_query(
            "INSERT INTO silver.test_data VALUES (4, 'd', 40.0)",
            "attempt to insert row",
        )
        assert "Error" in result

    def test_update_blocked(self, query_tools):
        """UPDATE is rejected with an error."""
        result = query_tools.run_custom_query(
            "UPDATE silver.test_data SET value = 0",
            "attempt to update rows",
        )
        assert "Error" in result

    def test_delete_blocked(self, query_tools):
        """DELETE is rejected with an error."""
        result = query_tools.run_custom_query(
            "DELETE FROM silver.test_data",
            "attempt to delete rows",
        )
        assert "Error" in result

    def test_create_blocked(self, query_tools):
        """CREATE TABLE is rejected with an error."""
        result = query_tools.run_custom_query(
            "CREATE TABLE silver.evil (x INT)",
            "attempt to create table",
        )
        assert "Error" in result

    def test_alter_blocked(self, query_tools):
        """ALTER TABLE is rejected with an error."""
        result = query_tools.run_custom_query(
            "ALTER TABLE silver.test_data ADD COLUMN z INT",
            "attempt to alter table",
        )
        assert "Error" in result

    def test_truncate_blocked(self, query_tools):
        """TRUNCATE is rejected with an error."""
        result = query_tools.run_custom_query(
            "TRUNCATE silver.test_data",
            "attempt to truncate table",
        )
        assert "Error" in result


# ---------------------------------------------------------------------------
# Case sensitivity
# ---------------------------------------------------------------------------


class TestCaseInsensitivity:
    """Keyword check must be case-insensitive."""

    def test_lowercase_drop_blocked(self, query_tools):
        """Lowercase 'drop table' is still rejected."""
        result = query_tools.run_custom_query(
            "drop table silver.test_data",
            "lowercase drop attempt",
        )
        assert "Error" in result

    def test_mixed_case_insert_blocked(self, query_tools):
        """Mixed-case 'Insert' is still rejected."""
        result = query_tools.run_custom_query(
            "Insert INTO silver.test_data VALUES (5, 'e', 50.0)",
            "mixed-case insert attempt",
        )
        assert "Error" in result


# ---------------------------------------------------------------------------
# Word boundary — substrings must not trigger false positives
# ---------------------------------------------------------------------------


class TestWordBoundary:
    """Forbidden keyword appearing as a substring must not be blocked."""

    def test_keyword_substring_passes_filter(self, query_tools):
        """'updated_at' contains 'UPDATE' but must not be blocked by the filter.

        The query will fail at execution because the column does not exist,
        but the error must come from DuckDB — not from the keyword filter.
        """
        result = query_tools.run_custom_query(
            "SELECT updated_at FROM silver.test_data",
            "column name containing forbidden substring",
        )
        # Keyword filter must NOT have triggered — no "forbidden keyword" message
        assert "forbidden keyword" not in result.lower()
        # The query fails at execution level (column missing) → error from DuckDB path
        assert "Error" in result


# ---------------------------------------------------------------------------
# Multi-statement injection
# ---------------------------------------------------------------------------


class TestMultiStatementInjection:
    """Semicolon-separated injection must be caught."""

    def test_multi_statement_blocked(self, query_tools):
        """Injected DROP after a valid SELECT is rejected."""
        result = query_tools.run_custom_query(
            "SELECT 1; DROP TABLE silver.test_data",
            "semicolon-separated injection",
        )
        assert "Error" in result
        assert "DROP" in result or "forbidden" in result.lower()


# ---------------------------------------------------------------------------
# Malformed / empty SQL
# ---------------------------------------------------------------------------


class TestMalformedAndEmptySQL:
    """Invalid SQL should produce an execution error, not crash."""

    def test_empty_sql_error(self, query_tools):
        """Empty string causes a DuckDB execution error."""
        result = query_tools.run_custom_query("", "empty query string")
        assert "Error" in result

    def test_malformed_sql_error(self, query_tools):
        """Typo in SELECT keyword causes an execution error."""
        result = query_tools.run_custom_query(
            "SELCT * FROM silver.test_data",
            "malformed sql keyword",
        )
        assert "Error" in result
