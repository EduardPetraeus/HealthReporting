"""
Tests for the Withings connector.
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


# Connector root paths
_REPO_ROOT = Path(__file__).resolve().parents[1]
_WITHINGS_DIR = (
    _REPO_ROOT
    / "health_unified_platform"
    / "health_platform"
    / "source_connectors"
    / "withings"
)
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


# Load modules from the Withings connector directory specifically
withings_client = _load_module("withings_client", _WITHINGS_DIR / "client.py")
withings_auth = _load_module("withings_auth", _WITHINGS_DIR / "auth.py")


class TestWithingsClient:
    """Tests for WithingsClient instantiation and endpoint listing."""

    def test_client_instantiation(self) -> None:
        client = withings_client.WithingsClient(access_token="test_token_abc123")
        assert client.access_token == "test_token_abc123"

    def test_client_session_created(self) -> None:
        client = withings_client.WithingsClient(access_token="test_token_abc123")
        assert client.session is not None

    def test_get_endpoints(self) -> None:
        client = withings_client.WithingsClient(access_token="test_token_abc123")
        endpoints = client.get_endpoints()
        assert "weight" in endpoints
        assert "blood_pressure" in endpoints
        assert "temperature" in endpoints
        assert len(endpoints) == 6

    def test_measure_types_defined(self) -> None:
        assert "weight_kg" in withings_client.MEASURE_TYPES
        assert "fat_ratio_pct" in withings_client.MEASURE_TYPES
        assert "systolic_mmhg" in withings_client.MEASURE_TYPES
        assert "diastolic_mmhg" in withings_client.MEASURE_TYPES
        assert len(withings_client.BODY_MEASURE_TYPES) == 7
        assert len(withings_client.BLOOD_PRESSURE_TYPES) == 3

    def test_base_url(self) -> None:
        assert withings_client.BASE_URL == "https://wbsapi.withings.net"

    def test_date_to_epoch(self) -> None:
        from datetime import date

        client = withings_client.WithingsClient(access_token="test_token_abc123")
        epoch = client._date_to_epoch(date(2026, 1, 1))
        assert isinstance(epoch, int)
        assert epoch > 0


class TestWithingsAuth:
    """Tests for Withings OAuth configuration and token paths."""

    def test_token_file_path_uses_home(self) -> None:
        assert str(withings_auth.TOKEN_FILE).startswith(str(Path.home()))
        assert "withings_tokens.json" in str(withings_auth.TOKEN_FILE)
        # Must NOT contain hardcoded /Users/ path
        assert "/Users/" not in str(withings_auth.TOKEN_FILE).replace(
            str(Path.home()), ""
        )

    def test_config_dir_path(self) -> None:
        expected = Path.home() / ".config" / "health_reporting"
        assert withings_auth.CONFIG_DIR == expected

    def test_authorize_url(self) -> None:
        assert (
            withings_auth.AUTHORIZE_URL
            == "https://account.withings.com/oauth2_user/authorize2"
        )

    def test_token_url(self) -> None:
        assert withings_auth.TOKEN_URL == "https://wbsapi.withings.net/v2/oauth2"

    def test_callback_port(self) -> None:
        assert isinstance(withings_auth.CALLBACK_PORT, int)
        assert withings_auth.CALLBACK_PORT == 8081

    def test_oauth_scopes(self) -> None:
        assert "user.metrics" in withings_auth.OAUTH_SCOPES
        assert "user.activity" in withings_auth.OAUTH_SCOPES

    def test_load_tokens_returns_none_when_no_file(self, tmp_path: Path) -> None:
        with patch.object(withings_auth, "TOKEN_FILE", tmp_path / "nonexistent.json"):
            result = withings_auth._load_tokens()
            assert result is None

    def test_is_expired_with_expired_token(self) -> None:
        expired_tokens = {"expires_at": 0}
        assert withings_auth._is_expired(expired_tokens) is True

    def test_is_expired_with_fresh_token(self) -> None:
        fresh_tokens = {"expires_at": time.time() + 3600}
        assert withings_auth._is_expired(fresh_tokens) is False

    def test_save_tokens_creates_file(self, tmp_path: Path) -> None:
        test_file = tmp_path / "test_tokens.json"
        with patch.object(withings_auth, "TOKEN_FILE", test_file), patch.object(
            withings_auth, "CONFIG_DIR", tmp_path
        ):
            withings_auth._save_tokens(
                {"access_token": "test", "expires_at": 9999999999}
            )
            assert test_file.exists()
            data = json.loads(test_file.read_text())
            assert data["access_token"] == "test"
