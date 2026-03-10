"""Tests for health_platform.source_connectors.oura.auth."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))

import health_platform.source_connectors.oura.auth as auth_module
from health_platform.source_connectors.oura.auth import (
    _is_expired,
    _load_credentials,
    _load_tokens,
    _refresh_tokens,
    _save_tokens,
    get_access_token,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FAKE_CREDENTIALS = {
    "OURA_CLIENT_ID": "test_client_id",
    "OURA_CLIENT_SECRET": "test_client_secret",
}


def _make_tokens(
    expires_at: float, access_token: str = "acc_abc", refresh_token: str = "ref_xyz"
) -> dict:
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "Bearer",
        "expires_at": expires_at,
    }


# ---------------------------------------------------------------------------
# _load_credentials
# ---------------------------------------------------------------------------


class TestLoadCredentials:
    @patch("health_platform.source_connectors.oura.auth.get_secret")
    def test_load_credentials_success(self, mock_secret, monkeypatch):
        """Returns (client_id, client_secret, redirect_uri) when secrets exist."""
        mock_secret.side_effect = lambda key, **kw: FAKE_CREDENTIALS.get(key)
        monkeypatch.delenv("OURA_REDIRECT_URI", raising=False)

        client_id, client_secret, redirect_uri = _load_credentials()

        assert client_id == "test_client_id"
        assert client_secret == "test_client_secret"
        assert redirect_uri == "http://localhost:8080/callback"

    @patch("health_platform.source_connectors.oura.auth.get_secret")
    def test_load_credentials_missing_raises(self, mock_secret, monkeypatch):
        """Raises ValueError when client_id or client_secret is None."""
        mock_secret.return_value = None
        monkeypatch.delenv("OURA_REDIRECT_URI", raising=False)

        with pytest.raises(ValueError, match="Missing OURA_CLIENT_ID"):
            _load_credentials()

    @patch("health_platform.source_connectors.oura.auth.get_secret")
    def test_redirect_uri_env_override(self, mock_secret, monkeypatch):
        """Uses OURA_REDIRECT_URI env var when set instead of the default."""
        mock_secret.side_effect = lambda key, **kw: FAKE_CREDENTIALS.get(key)
        monkeypatch.setenv("OURA_REDIRECT_URI", "http://custom-host:9000/cb")

        _, _, redirect_uri = _load_credentials()

        assert redirect_uri == "http://custom-host:9000/cb"

    @patch("health_platform.source_connectors.oura.auth.get_secret")
    def test_load_credentials_empty_string_raises(self, mock_secret, monkeypatch):
        """Raises ValueError when client_id is an empty string (falsy)."""
        mock_secret.side_effect = lambda key, **kw: (
            "" if key == "OURA_CLIENT_ID" else "test_secret"
        )
        monkeypatch.delenv("OURA_REDIRECT_URI", raising=False)

        with pytest.raises(ValueError):
            _load_credentials()


# ---------------------------------------------------------------------------
# _save_tokens / _load_tokens
# ---------------------------------------------------------------------------


class TestSaveAndLoadTokens:
    def test_save_tokens_creates_file(self, tmp_path, monkeypatch):
        """Token file is created with correct content and 0o600 permissions."""
        monkeypatch.setattr(auth_module, "TOKEN_FILE", tmp_path / "oura_tokens.json")
        monkeypatch.setattr(auth_module, "CONFIG_DIR", tmp_path)

        tokens = _make_tokens(expires_at=9999999999.0)
        _save_tokens(tokens)

        token_file = tmp_path / "oura_tokens.json"
        assert token_file.exists()
        assert json.loads(token_file.read_text()) == tokens
        assert oct(token_file.stat().st_mode)[-3:] == "600"

    def test_load_tokens_missing_returns_none(self, tmp_path, monkeypatch):
        """Returns None when no token file exists."""
        monkeypatch.setattr(auth_module, "TOKEN_FILE", tmp_path / "oura_tokens.json")

        result = _load_tokens()

        assert result is None

    def test_load_tokens_valid_json(self, tmp_path, monkeypatch):
        """Returns parsed dict when token file exists and contains valid JSON."""
        token_file = tmp_path / "oura_tokens.json"
        tokens = _make_tokens(expires_at=9999999999.0)
        token_file.write_text(json.dumps(tokens))
        monkeypatch.setattr(auth_module, "TOKEN_FILE", token_file)

        result = _load_tokens()

        assert result == tokens


# ---------------------------------------------------------------------------
# _is_expired
# ---------------------------------------------------------------------------


class TestIsExpired:
    def test_is_expired_past(self):
        """Returns True when expires_at is far in the past."""
        tokens = _make_tokens(expires_at=1000.0)
        assert _is_expired(tokens) is True

    def test_is_expired_future(self):
        """Returns False when expires_at is far in the future."""
        tokens = _make_tokens(expires_at=time.time() + 3600)
        assert _is_expired(tokens) is False

    def test_is_expired_60s_buffer(self):
        """Returns True when token expires within 60 seconds (grace period)."""
        tokens = _make_tokens(expires_at=time.time() + 30)
        assert _is_expired(tokens) is True

    def test_is_expired_missing_expires_at(self):
        """Returns True when expires_at key is absent (defaults to 0)."""
        assert _is_expired({}) is True


# ---------------------------------------------------------------------------
# _refresh_tokens
# ---------------------------------------------------------------------------


class TestRefreshTokens:
    @patch("health_platform.source_connectors.oura.auth.requests.post")
    def test_refresh_success(self, mock_post, tmp_path, monkeypatch):
        """Successful refresh saves tokens and returns dict with expires_at."""
        monkeypatch.setattr(auth_module, "TOKEN_FILE", tmp_path / "oura_tokens.json")
        monkeypatch.setattr(auth_module, "CONFIG_DIR", tmp_path)

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "new_acc",
            "refresh_token": "new_ref",
            "expires_in": 86400,
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        existing = _make_tokens(expires_at=1000.0)
        result = _refresh_tokens(existing, "cid", "csecret")

        assert result["access_token"] == "new_acc"
        assert result["refresh_token"] == "new_ref"
        assert result["expires_at"] > time.time()
        assert (tmp_path / "oura_tokens.json").exists()

    @patch("health_platform.source_connectors.oura.auth.requests.post")
    def test_refresh_failure_raises(self, mock_post, tmp_path, monkeypatch):
        """HTTP error from token endpoint propagates as HTTPError."""
        monkeypatch.setattr(auth_module, "TOKEN_FILE", tmp_path / "oura_tokens.json")
        monkeypatch.setattr(auth_module, "CONFIG_DIR", tmp_path)

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "401 Unauthorized"
        )
        mock_post.return_value = mock_response

        existing = _make_tokens(expires_at=1000.0)
        with pytest.raises(requests.exceptions.HTTPError):
            _refresh_tokens(existing, "cid", "csecret")


# ---------------------------------------------------------------------------
# get_access_token
# ---------------------------------------------------------------------------


class TestGetAccessToken:
    @patch("health_platform.source_connectors.oura.auth.get_secret")
    def test_get_access_token_cached(self, mock_secret, tmp_path, monkeypatch):
        """Returns cached access_token directly when tokens are valid (no HTTP)."""
        mock_secret.side_effect = lambda key, **kw: FAKE_CREDENTIALS.get(key)
        monkeypatch.delenv("OURA_REDIRECT_URI", raising=False)

        token_file = tmp_path / "oura_tokens.json"
        valid_tokens = _make_tokens(
            expires_at=time.time() + 3600, access_token="cached_token"
        )
        token_file.write_text(json.dumps(valid_tokens))
        monkeypatch.setattr(auth_module, "TOKEN_FILE", token_file)
        monkeypatch.setattr(auth_module, "CONFIG_DIR", tmp_path)

        with patch(
            "health_platform.source_connectors.oura.auth.requests.post"
        ) as mock_post:
            result = get_access_token()
            mock_post.assert_not_called()

        assert result == "cached_token"

    @patch("health_platform.source_connectors.oura.auth.get_secret")
    @patch("health_platform.source_connectors.oura.auth.requests.post")
    def test_get_access_token_expired_refreshes(
        self, mock_post, mock_secret, tmp_path, monkeypatch
    ):
        """Refreshes expired tokens and returns the new access_token."""
        mock_secret.side_effect = lambda key, **kw: FAKE_CREDENTIALS.get(key)
        monkeypatch.delenv("OURA_REDIRECT_URI", raising=False)

        token_file = tmp_path / "oura_tokens.json"
        expired_tokens = _make_tokens(expires_at=1000.0, access_token="old_token")
        token_file.write_text(json.dumps(expired_tokens))
        monkeypatch.setattr(auth_module, "TOKEN_FILE", token_file)
        monkeypatch.setattr(auth_module, "CONFIG_DIR", tmp_path)

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "access_token": "refreshed_token",
            "refresh_token": "new_ref",
            "expires_in": 86400,
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        result = get_access_token()

        mock_post.assert_called_once()
        assert result == "refreshed_token"
