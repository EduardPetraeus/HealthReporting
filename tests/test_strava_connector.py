"""
Tests for the Strava connector.
Tests class instantiation, endpoint listing, token path resolution,
and OAuth URL structure. No actual API calls are made.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

# Connector root paths
_REPO_ROOT = Path(__file__).resolve().parents[1]
_STRAVA_DIR = _REPO_ROOT / "health_unified_platform" / "health_platform" / "source_connectors" / "strava"
_PLATFORM_DIR = _REPO_ROOT / "health_unified_platform" / "health_platform"

# Ensure platform utils are importable
if str(_PLATFORM_DIR.parent) not in sys.path:
    sys.path.insert(0, str(_PLATFORM_DIR.parent))


def _load_module(module_name: str, file_path: Path):
    """Load a module from a specific file path to avoid name collisions."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Load modules from the Strava connector directory specifically
strava_client = _load_module("strava_client", _STRAVA_DIR / "client.py")
strava_auth = _load_module("strava_auth", _STRAVA_DIR / "auth.py")


class TestStravaClient:
    """Tests for StravaClient instantiation and endpoint listing."""

    def test_client_instantiation(self) -> None:
        client = strava_client.StravaClient(access_token="test_token_abc123")
        assert client.session is not None

    def test_client_auth_header(self) -> None:
        client = strava_client.StravaClient(access_token="test_token_abc123")
        assert client.session.headers["Authorization"] == "Bearer test_token_abc123"

    def test_get_endpoints(self) -> None:
        client = strava_client.StravaClient(access_token="test_token_abc123")
        endpoints = client.get_endpoints()
        assert "activities" in endpoints
        assert "athlete_stats" in endpoints
        assert len(endpoints) == 2

    def test_base_url(self) -> None:
        assert strava_client.BASE_URL == "https://www.strava.com/api/v3"

    def test_page_size(self) -> None:
        assert strava_client.PAGE_SIZE == 100

    def test_date_to_epoch(self) -> None:
        from datetime import date

        client = strava_client.StravaClient(access_token="test_token_abc123")
        epoch = client._date_to_epoch(date(2026, 1, 1))
        assert isinstance(epoch, int)
        assert epoch > 0


class TestStravaAuth:
    """Tests for Strava OAuth configuration and token paths."""

    def test_token_file_path_uses_home(self) -> None:
        assert str(strava_auth.TOKEN_FILE).startswith(str(Path.home()))
        assert "strava_tokens.json" in str(strava_auth.TOKEN_FILE)
        # Must NOT contain hardcoded /Users/ path
        assert "/Users/" not in str(strava_auth.TOKEN_FILE).replace(str(Path.home()), "")

    def test_config_dir_path(self) -> None:
        expected = Path.home() / ".config" / "health_reporting"
        assert strava_auth.CONFIG_DIR == expected

    def test_authorize_url(self) -> None:
        assert strava_auth.AUTHORIZE_URL == "https://www.strava.com/oauth/authorize"

    def test_token_url(self) -> None:
        assert strava_auth.TOKEN_URL == "https://www.strava.com/oauth/token"

    def test_callback_port(self) -> None:
        assert isinstance(strava_auth.CALLBACK_PORT, int)
        assert strava_auth.CALLBACK_PORT == 8082

    def test_oauth_scopes(self) -> None:
        assert "activity:read_all" in strava_auth.OAUTH_SCOPES

    def test_load_tokens_returns_none_when_no_file(self, tmp_path: Path) -> None:
        with patch.object(strava_auth, "TOKEN_FILE", tmp_path / "nonexistent.json"):
            result = strava_auth._load_tokens()
            assert result is None

    def test_is_expired_with_expired_token(self) -> None:
        expired_tokens = {"expires_at": 0}
        assert strava_auth._is_expired(expired_tokens) is True

    def test_is_expired_with_fresh_token(self) -> None:
        fresh_tokens = {"expires_at": time.time() + 3600}
        assert strava_auth._is_expired(fresh_tokens) is False

    def test_save_tokens_creates_file(self, tmp_path: Path) -> None:
        test_file = tmp_path / "test_tokens.json"
        with patch.object(strava_auth, "TOKEN_FILE", test_file), \
             patch.object(strava_auth, "CONFIG_DIR", tmp_path):
            strava_auth._save_tokens({"access_token": "test", "expires_at": 9999999999})
            assert test_file.exists()
            data = json.loads(test_file.read_text())
            assert data["access_token"] == "test"
