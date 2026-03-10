"""
Unit tests for health_platform.source_connectors.withings.auth

Covers: _load_credentials, _save_tokens, _load_tokens, _is_expired,
        _refresh_tokens, get_access_token — including Withings-specific
        action=requesttoken quirk and body-envelope unwrapping.
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest
import requests
from health_platform.source_connectors.withings.auth import (
    _is_expired,
    _load_credentials,
    _load_tokens,
    _refresh_tokens,
    _save_tokens,
    get_access_token,
)

MODULE = "health_platform.source_connectors.withings.auth"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_valid_tokens(offset: int = 7200) -> dict:
    """Return a token dict whose expires_at is `offset` seconds from now."""
    return {
        "access_token": "acc_withings",
        "refresh_token": "ref_withings",
        "expires_at": time.time() + offset,
    }


def _mock_post_response(payload: dict, status_code: int = 200) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = payload
    if status_code >= 400:
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)
    else:
        mock_resp.raise_for_status.return_value = None
    return mock_resp


# ---------------------------------------------------------------------------
# _load_credentials
# ---------------------------------------------------------------------------


class TestLoadCredentials:
    def test_load_credentials_success(self, monkeypatch):
        monkeypatch.delenv("WITHINGS_REDIRECT_URI", raising=False)
        with patch(f"{MODULE}.get_secret") as mock_secret:
            mock_secret.side_effect = lambda key, **_: (
                "wid123" if key == "WITHINGS_CLIENT_ID" else "wsec456"
            )
            client_id, client_secret, redirect_uri = _load_credentials()

        assert client_id == "wid123"
        assert client_secret == "wsec456"
        assert redirect_uri == "http://localhost:8081/callback"

    def test_load_credentials_env_override_redirect_uri(self, monkeypatch):
        monkeypatch.setenv("WITHINGS_REDIRECT_URI", "https://example.com/cb")
        with patch(f"{MODULE}.get_secret") as mock_secret:
            mock_secret.side_effect = lambda key, **_: (
                "wid123" if key == "WITHINGS_CLIENT_ID" else "wsec456"
            )
            _, _, redirect_uri = _load_credentials()

        assert redirect_uri == "https://example.com/cb"

    def test_load_credentials_missing_id_raises(self, monkeypatch):
        monkeypatch.delenv("WITHINGS_REDIRECT_URI", raising=False)
        with patch(f"{MODULE}.get_secret") as mock_secret:
            mock_secret.side_effect = lambda key, **_: (
                None if key == "WITHINGS_CLIENT_ID" else "wsec456"
            )
            with pytest.raises(ValueError, match="WITHINGS_CLIENT_ID"):
                _load_credentials()

    def test_load_credentials_missing_secret_raises(self, monkeypatch):
        monkeypatch.delenv("WITHINGS_REDIRECT_URI", raising=False)
        with patch(f"{MODULE}.get_secret") as mock_secret:
            mock_secret.side_effect = lambda key, **_: (
                "wid123" if key == "WITHINGS_CLIENT_ID" else None
            )
            with pytest.raises(ValueError, match="WITHINGS_CLIENT_SECRET"):
                _load_credentials()


# ---------------------------------------------------------------------------
# _save_tokens / _load_tokens
# ---------------------------------------------------------------------------


class TestSaveLoadTokens:
    def test_save_tokens_creates_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", tmp_path / "withings_tokens.json")
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        tokens = {"access_token": "x", "expires_at": 9999999999}
        _save_tokens(tokens)

        token_file = tmp_path / "withings_tokens.json"
        assert token_file.exists()
        assert json.loads(token_file.read_text()) == tokens

    def test_save_tokens_sets_chmod_600(self, tmp_path, monkeypatch):
        token_file = tmp_path / "withings_tokens.json"
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        _save_tokens({"access_token": "x"})

        mode = oct(token_file.stat().st_mode)[-3:]
        assert mode == "600"

    def test_load_tokens_missing_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", tmp_path / "nonexistent.json")
        assert _load_tokens() is None

    def test_load_tokens_valid_json(self, tmp_path, monkeypatch):
        token_file = tmp_path / "withings_tokens.json"
        data = {"access_token": "abc", "expires_at": 1234567890}
        token_file.write_text(json.dumps(data))
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)

        result = _load_tokens()
        assert result == data


# ---------------------------------------------------------------------------
# _is_expired
# ---------------------------------------------------------------------------


class TestIsExpired:
    def test_is_expired_past(self):
        tokens = {"expires_at": time.time() - 1}
        assert _is_expired(tokens) is True

    def test_is_expired_future(self):
        tokens = {"expires_at": time.time() + 3600}
        assert _is_expired(tokens) is False

    def test_is_expired_within_60s_buffer(self):
        # expires_at is 30 s from now — within the 60 s buffer → expired
        tokens = {"expires_at": time.time() + 30}
        assert _is_expired(tokens) is True

    def test_is_expired_missing_key_treated_as_expired(self):
        # No expires_at → defaults to 0 → always expired
        assert _is_expired({}) is True


# ---------------------------------------------------------------------------
# _refresh_tokens
# ---------------------------------------------------------------------------


class TestRefreshTokens:
    def test_refresh_success(self, tmp_path, monkeypatch):
        token_file = tmp_path / "withings_tokens.json"
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        payload = {
            "body": {
                "access_token": "new_acc",
                "refresh_token": "new_ref",
                "expires_in": 10800,
            }
        }
        mock_resp = _mock_post_response(payload)

        with patch(f"{MODULE}.requests.post", return_value=mock_resp):
            result = _refresh_tokens({"refresh_token": "old_ref"}, "wid", "wsec")

        assert result["access_token"] == "new_acc"
        assert result["refresh_token"] == "new_ref"
        assert result["expires_at"] == pytest.approx(time.time() + 10800, abs=5)

    def test_refresh_failure_raises_http_error(self, monkeypatch):
        mock_resp = _mock_post_response({}, status_code=401)

        with patch(f"{MODULE}.requests.post", return_value=mock_resp):
            with pytest.raises(requests.HTTPError):
                _refresh_tokens({"refresh_token": "old"}, "wid", "wsec")

    def test_refresh_includes_action_requesttoken(self, tmp_path, monkeypatch):
        """Withings requires action=requesttoken in the POST body."""
        token_file = tmp_path / "withings_tokens.json"
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        payload = {"body": {"access_token": "a", "refresh_token": "b"}}
        mock_resp = _mock_post_response(payload)

        with patch(f"{MODULE}.requests.post", return_value=mock_resp) as mock_post:
            _refresh_tokens({"refresh_token": "old"}, "wid", "wsec")

        call_data = mock_post.call_args[1].get("data", {})
        assert call_data.get("action") == "requesttoken"

    def test_refresh_unwraps_body_envelope(self, tmp_path, monkeypatch):
        """Response wrapped in {body: {...}} must be unwrapped correctly."""
        token_file = tmp_path / "withings_tokens.json"
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        payload = {
            "status": 0,
            "body": {
                "access_token": "unwrapped_acc",
                "refresh_token": "unwrapped_ref",
                "expires_in": 10800,
            },
        }
        mock_resp = _mock_post_response(payload)

        with patch(f"{MODULE}.requests.post", return_value=mock_resp):
            result = _refresh_tokens({"refresh_token": "old"}, "wid", "wsec")

        assert result["access_token"] == "unwrapped_acc"
        # 'status' key from outer envelope must NOT leak in
        assert "status" not in result

    def test_default_expiry_10800(self, tmp_path, monkeypatch):
        """When expires_in is absent, fallback must be 10800 (Withings default)."""
        token_file = tmp_path / "withings_tokens.json"
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        # Body has no expires_in
        payload = {"body": {"access_token": "a", "refresh_token": "b"}}
        mock_resp = _mock_post_response(payload)

        with patch(f"{MODULE}.requests.post", return_value=mock_resp):
            result = _refresh_tokens({"refresh_token": "old"}, "wid", "wsec")

        assert result["expires_at"] == pytest.approx(time.time() + 10800, abs=5)


# ---------------------------------------------------------------------------
# get_access_token
# ---------------------------------------------------------------------------


class TestGetAccessToken:
    def test_get_access_token_cached(self, tmp_path, monkeypatch):
        """Valid cached token must be returned without any HTTP call."""
        tokens = _make_valid_tokens(offset=7200)
        token_file = tmp_path / "withings_tokens.json"
        token_file.write_text(json.dumps(tokens))
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        with (
            patch(f"{MODULE}.get_secret") as mock_secret,
            patch(f"{MODULE}.requests.post") as mock_post,
        ):
            mock_secret.side_effect = lambda k, **_: (
                "wid" if k == "WITHINGS_CLIENT_ID" else "wsec"
            )
            result = get_access_token()

        assert result == "acc_withings"
        mock_post.assert_not_called()

    def test_get_access_token_expired_refreshes(self, tmp_path, monkeypatch):
        """Expired token must trigger _refresh_tokens and return new token."""
        expired = _make_valid_tokens(offset=-100)
        token_file = tmp_path / "withings_tokens.json"
        token_file.write_text(json.dumps(expired))
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        refresh_payload = {
            "body": {
                "access_token": "refreshed_acc",
                "refresh_token": "refreshed_ref",
                "expires_in": 10800,
            }
        }
        mock_resp = _mock_post_response(refresh_payload)

        with (
            patch(f"{MODULE}.get_secret") as mock_secret,
            patch(f"{MODULE}.requests.post", return_value=mock_resp),
        ):
            mock_secret.side_effect = lambda k, **_: (
                "wid" if k == "WITHINGS_CLIENT_ID" else "wsec"
            )
            result = get_access_token()

        assert result == "refreshed_acc"
