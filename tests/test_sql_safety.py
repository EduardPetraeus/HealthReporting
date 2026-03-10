"""Tests for health_platform.utils.sql_safety — canonical SQL identifier validation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "health_unified_platform"),
)

from health_platform.utils.sql_safety import _SAFE_IDENTIFIER, validate_sql_identifier


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
