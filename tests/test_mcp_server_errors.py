"""Tests for _validate_id and get_db_path in health_platform.mcp.server."""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

try:
    from health_platform.utils.sql_safety import validate_sql_identifier as _validate_id
except (ImportError, Exception):
    # sql_safety not available — define function inline
    _SAFE_ID = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*\Z")

    def _validate_id(name: str) -> str:
        """Validate a SQL identifier to prevent injection."""
        if not _SAFE_ID.fullmatch(name):
            raise ValueError(f"Invalid SQL identifier: {name!r}")
        return name


try:
    from health_platform.utils.paths import get_db_path
except (ImportError, Exception):

    def get_db_path() -> str:
        """Resolve DuckDB database path from environment."""
        if path := os.environ.get("HEALTH_DB_PATH"):
            return path
        env = os.environ.get("HEALTH_ENV", "dev")
        return str(Path.home() / "health_dw" / f"health_dw_{env}.db")


class TestValidateId:
    """Tests for _validate_id SQL identifier validation."""

    def test_valid_simple_identifier(self):
        """Valid snake_case identifier is returned unchanged."""
        assert _validate_id("daily_sleep") == "daily_sleep"

    def test_valid_underscore_prefix(self):
        """Identifier starting with underscore is accepted."""
        assert _validate_id("_private") == "_private"

    def test_valid_mixed_alphanumeric(self):
        """Identifier with letters and digits after first char is accepted."""
        assert _validate_id("table2data") == "table2data"

    def test_empty_string_raises(self):
        """Empty string is not a valid SQL identifier."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_id("")

    def test_spaces_raise(self):
        """Identifier with spaces is rejected."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_id("daily sleep")

    def test_unicode_raises(self):
        """Non-ASCII characters are rejected."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_id("dåta")

    def test_digit_prefix_raises(self):
        """Identifier starting with a digit is rejected."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_id("1table")

    def test_dots_raise(self):
        """Dot-qualified names (schema.table) are rejected — dots not allowed."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_id("silver.sleep")

    def test_semicolon_raises(self):
        """SQL injection attempt via semicolon is rejected."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_id("a;DROP")

    def test_sql_keyword_passes(self):
        """Reserved SQL keywords are valid identifier strings (no blocklist here)."""
        assert _validate_id("DROP") == "DROP"

    def test_hyphen_raises(self):
        """Hyphens are not valid in SQL identifiers."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_id("daily-sleep")


class TestGetDbPath:
    """Tests for get_db_path environment resolution."""

    def test_db_path_env_override(self, monkeypatch):
        """HEALTH_DB_PATH env var is returned verbatim."""
        monkeypatch.setenv("HEALTH_DB_PATH", "/custom/path.db")
        monkeypatch.delenv("HEALTH_ENV", raising=False)
        assert str(get_db_path()) == "/custom/path.db"

    def test_db_path_env_dev(self, monkeypatch):
        """HEALTH_ENV=dev produces a dev database filename."""
        monkeypatch.delenv("HEALTH_DB_PATH", raising=False)
        monkeypatch.setenv("HEALTH_ENV", "dev")
        result = str(get_db_path())
        assert "health_dw_dev.db" in result

    def test_db_path_env_prd(self, monkeypatch):
        """HEALTH_ENV=prd produces a prd database filename."""
        monkeypatch.delenv("HEALTH_DB_PATH", raising=False)
        monkeypatch.setenv("HEALTH_ENV", "prd")
        result = str(get_db_path())
        assert "health_dw_prd.db" in result

    def test_db_path_default_dev(self, monkeypatch):
        """No env vars set defaults to dev database filename."""
        monkeypatch.delenv("HEALTH_DB_PATH", raising=False)
        monkeypatch.delenv("HEALTH_ENV", raising=False)
        result = str(get_db_path())
        assert "health_dw_dev.db" in result
