"""Tests for the shared macOS Keychain utility.

All tests are mocked — no real keychain access needed.
"""

from __future__ import annotations

import subprocess
from unittest.mock import patch

import pytest  # noqa: F401
from health_platform.utils.keychain import get_secret

pytestmark = pytest.mark.integration


class TestGetSecret:
    """Tests for get_secret()."""

    def test_keychain_returns_value(self):
        """Keychain has the secret — returns it directly."""
        mock_result = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="my-secret-value\n", stderr=""
        )
        with patch(
            "health_platform.utils.keychain.subprocess.run", return_value=mock_result
        ):
            result = get_secret("TEST_KEY")
        assert result == "my-secret-value"

    def test_keychain_fails_falls_back_to_env(self, monkeypatch):
        """Keychain fails, env var is set — falls back to env."""
        mock_result = subprocess.CompletedProcess(
            args=[], returncode=44, stdout="", stderr="not found"
        )
        monkeypatch.setenv("TEST_KEY", "env-value")
        with patch(
            "health_platform.utils.keychain.subprocess.run", return_value=mock_result
        ):
            result = get_secret("TEST_KEY")
        assert result == "env-value"

    def test_keychain_fails_no_env_returns_none(self, monkeypatch):
        """Keychain fails, no env var — returns None."""
        mock_result = subprocess.CompletedProcess(
            args=[], returncode=44, stdout="", stderr="not found"
        )
        monkeypatch.delenv("TEST_KEY", raising=False)
        with patch(
            "health_platform.utils.keychain.subprocess.run", return_value=mock_result
        ):
            result = get_secret("TEST_KEY")
        assert result is None

    def test_fallback_env_false_returns_none(self, monkeypatch):
        """Keychain fails, fallback_env=False — returns None even with env var."""
        mock_result = subprocess.CompletedProcess(
            args=[], returncode=44, stdout="", stderr="not found"
        )
        monkeypatch.setenv("TEST_KEY", "env-value")
        with patch(
            "health_platform.utils.keychain.subprocess.run", return_value=mock_result
        ):
            result = get_secret("TEST_KEY", fallback_env=False)
        assert result is None

    def test_subprocess_exception_falls_back_gracefully(self, monkeypatch):
        """Subprocess raises exception — falls back to env gracefully."""
        monkeypatch.setenv("TEST_KEY", "env-fallback")
        with patch(
            "health_platform.utils.keychain.subprocess.run",
            side_effect=OSError("security binary not found"),
        ):
            result = get_secret("TEST_KEY")
        assert result == "env-fallback"

    def test_subprocess_exception_no_env_returns_none(self, monkeypatch):
        """Subprocess raises exception, no env — returns None."""
        monkeypatch.delenv("TEST_KEY", raising=False)
        with patch(
            "health_platform.utils.keychain.subprocess.run",
            side_effect=OSError("security binary not found"),
        ):
            result = get_secret("TEST_KEY")
        assert result is None

    def test_keychain_returns_empty_string_falls_back(self, monkeypatch):
        """Keychain returns empty stdout — treats as not found."""
        mock_result = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="  \n", stderr=""
        )
        monkeypatch.setenv("TEST_KEY", "env-value")
        with patch(
            "health_platform.utils.keychain.subprocess.run", return_value=mock_result
        ):
            result = get_secret("TEST_KEY")
        assert result == "env-value"
