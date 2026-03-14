"""Tests for health_platform.utils.sql_safety — canonical SQL validation utilities."""

from __future__ import annotations

import pytest
from health_platform.utils.sql_safety import (
    _SAFE_IDENTIFIER,
    validate_sql_identifier,
    validate_where_clause,
)


class TestValidateSqlIdentifier:
    """Tests for validate_sql_identifier."""

    def test_valid_simple(self):
        assert validate_sql_identifier("daily_sleep") == "daily_sleep"

    def test_valid_underscore_prefix(self):
        assert validate_sql_identifier("_private") == "_private"

    def test_valid_alphanumeric(self):
        assert validate_sql_identifier("table2data") == "table2data"

    def test_valid_uppercase(self):
        assert validate_sql_identifier("DROP") == "DROP"

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_sql_identifier("")

    def test_spaces_raise(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_sql_identifier("daily sleep")

    def test_unicode_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_sql_identifier("dåta")

    def test_digit_prefix_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_sql_identifier("1table")

    def test_dots_raise(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_sql_identifier("silver.sleep")

    def test_semicolon_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_sql_identifier("a;DROP")

    def test_hyphen_raises(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            validate_sql_identifier("daily-sleep")


class TestSafeIdentifierRegex:
    """Tests for _SAFE_IDENTIFIER regex directly."""

    def test_matches_valid(self):
        assert _SAFE_IDENTIFIER.match("hello_world")

    def test_rejects_empty(self):
        assert not _SAFE_IDENTIFIER.match("")

    def test_rejects_special(self):
        assert not _SAFE_IDENTIFIER.match("foo-bar")


class TestValidateWhereClause:
    """Tests for validate_where_clause — defense-in-depth on config-sourced WHERE fragments."""

    # --- Valid clauses (typical value_range patterns) ---

    def test_simple_comparison(self):
        assert validate_where_clause("score < 0") == "score < 0"

    def test_or_conditions(self):
        clause = "score < 0 OR score > 100"
        assert validate_where_clause(clause) == clause

    def test_numeric_with_decimals(self):
        clause = "weight_kg < 30.0 OR weight_kg > 200.0"
        assert validate_where_clause(clause) == clause

    def test_negative_numbers(self):
        clause = "temperature_min < -60 OR temperature_min > 50"
        assert validate_where_clause(clause) == clause

    def test_and_conditions(self):
        clause = "bpm >= 20 AND bpm <= 250"
        assert validate_where_clause(clause) == clause

    def test_is_not_null(self):
        clause = "value IS NOT NULL"
        assert validate_where_clause(clause) == clause

    def test_between(self):
        clause = "score BETWEEN 0 AND 100"
        assert validate_where_clause(clause) == clause

    def test_in_list(self):
        clause = "status IN ('active', 'pending')"
        assert validate_where_clause(clause) == clause

    def test_like(self):
        clause = "name LIKE '%test%'"
        assert validate_where_clause(clause) == clause

    def test_parenthesized(self):
        clause = "(score < 0) OR (score > 100)"
        assert validate_where_clause(clause) == clause

    # --- Dangerous keywords ---

    def test_rejects_union(self):
        with pytest.raises(
            ValueError, match="disallowed token|disallowed keyword|invalid identifier"
        ):
            validate_where_clause("1=1 UNION SELECT * FROM secrets")

    def test_rejects_drop(self):
        with pytest.raises(
            ValueError, match="disallowed token|disallowed keyword|invalid identifier"
        ):
            validate_where_clause("1=1 DROP TABLE users")

    def test_rejects_delete(self):
        with pytest.raises(
            ValueError, match="disallowed token|disallowed keyword|invalid identifier"
        ):
            validate_where_clause("DELETE FROM users WHERE 1=1")

    def test_rejects_insert(self):
        with pytest.raises(
            ValueError, match="disallowed token|disallowed keyword|invalid identifier"
        ):
            validate_where_clause("INSERT INTO users VALUES (1)")

    def test_rejects_update(self):
        with pytest.raises(
            ValueError, match="disallowed token|disallowed keyword|invalid identifier"
        ):
            validate_where_clause("UPDATE users SET admin=1")

    def test_rejects_alter(self):
        with pytest.raises(
            ValueError, match="disallowed token|disallowed keyword|invalid identifier"
        ):
            validate_where_clause("1=1 ALTER TABLE users ADD col INT")

    def test_rejects_create(self):
        with pytest.raises(
            ValueError, match="disallowed token|disallowed keyword|invalid identifier"
        ):
            validate_where_clause("1=1 CREATE TABLE evil (id INT)")

    def test_rejects_exec(self):
        with pytest.raises(
            ValueError, match="disallowed token|disallowed keyword|invalid identifier"
        ):
            validate_where_clause("EXEC xp_cmdshell('whoami')")

    def test_rejects_execute(self):
        with pytest.raises(
            ValueError, match="disallowed token|disallowed keyword|invalid identifier"
        ):
            validate_where_clause("EXECUTE sp_configure")

    def test_rejects_truncate(self):
        with pytest.raises(
            ValueError, match="disallowed token|disallowed keyword|invalid identifier"
        ):
            validate_where_clause("TRUNCATE TABLE users")

    def test_rejects_grant(self):
        with pytest.raises(
            ValueError, match="disallowed token|disallowed keyword|invalid identifier"
        ):
            validate_where_clause("GRANT ALL ON users TO public")

    def test_rejects_revoke(self):
        with pytest.raises(
            ValueError, match="disallowed token|disallowed keyword|invalid identifier"
        ):
            validate_where_clause("REVOKE ALL ON users FROM public")

    def test_rejects_case_insensitive(self):
        with pytest.raises(
            ValueError, match="disallowed token|disallowed keyword|invalid identifier"
        ):
            validate_where_clause("union select 1")

    # --- Dangerous patterns ---

    def test_rejects_semicolon(self):
        with pytest.raises(ValueError, match="disallowed token"):
            validate_where_clause("1=1; SELECT 1")

    def test_rejects_line_comment(self):
        with pytest.raises(ValueError, match="disallowed token"):
            validate_where_clause("1=1 -- comment")

    def test_rejects_block_comment_open(self):
        with pytest.raises(ValueError, match="disallowed token"):
            validate_where_clause("1=1 /* comment")

    def test_rejects_block_comment_close(self):
        with pytest.raises(ValueError, match="disallowed token"):
            validate_where_clause("comment */ 1=1")

    # --- Edge cases ---

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="must not be empty"):
            validate_where_clause("")

    def test_rejects_whitespace_only(self):
        with pytest.raises(ValueError, match="must not be empty"):
            validate_where_clause("   ")
