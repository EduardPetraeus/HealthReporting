"""
Unit tests for health_platform.source_connectors.strava.auth

Covers: _load_credentials, _save_tokens, _load_tokens, _is_expired,
        _refresh_tokens, get_access_token — including Strava-specific
        approval_prompt=auto quirk, hardcoded redirect_uri, and
        expires_at-in-response behaviour.
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest
import requests
from health_platform.source_connectors.strava.auth import (
    _is_expired,
    _load_credentials,
    _load_tokens,
    _refresh_tokens,
    _save_tokens,
    get_access_token,
)

pytestmark = pytest.mark.integration

MODULE = "health_platform.source_connectors.strava.auth"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_valid_tokens(offset: int = 7200) -> dict:
    """Return a token dict whose expires_at is `offset` seconds from now."""
    return {
        "access_token": "acc_strava",
        "refresh_token": "ref_strava",
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
    def test_load_credentials_success(self):
        with patch(f"{MODULE}.get_secret") as mock_secret:
            mock_secret.side_effect = lambda key, **_: (
                "sid123" if key == "STRAVA_CLIENT_ID" else "ssec456"
            )
            client_id, client_secret, redirect_uri = _load_credentials()

        assert client_id == "sid123"
        assert client_secret == "ssec456"
        assert redirect_uri == "http://localhost:8082/callback"

    def test_no_redirect_uri_env_override(self, monkeypatch):
        """Strava hardcodes redirect_uri — env vars must have no effect."""
        monkeypatch.setenv("STRAVA_REDIRECT_URI", "https://should-be-ignored.com/cb")
        with patch(f"{MODULE}.get_secret") as mock_secret:
            mock_secret.side_effect = lambda key, **_: (
                "sid123" if key == "STRAVA_CLIENT_ID" else "ssec456"
            )
            _, _, redirect_uri = _load_credentials()

        # Must always be hardcoded localhost:8082
        assert redirect_uri == "http://localhost:8082/callback"

    def test_load_credentials_missing_id_raises(self):
        with patch(f"{MODULE}.get_secret") as mock_secret:
            mock_secret.side_effect = lambda key, **_: (
                None if key == "STRAVA_CLIENT_ID" else "ssec456"
            )
            with pytest.raises(ValueError, match="STRAVA_CLIENT_ID"):
                _load_credentials()

    def test_load_credentials_missing_secret_raises(self):
        with patch(f"{MODULE}.get_secret") as mock_secret:
            mock_secret.side_effect = lambda key, **_: (
                "sid123" if key == "STRAVA_CLIENT_ID" else None
            )
            with pytest.raises(ValueError, match="STRAVA_CLIENT_SECRET"):
                _load_credentials()


# ---------------------------------------------------------------------------
# _save_tokens / _load_tokens
# ---------------------------------------------------------------------------


class TestSaveLoadTokens:
    def test_save_tokens_creates_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", tmp_path / "strava_tokens.json")
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        tokens = {"access_token": "x", "expires_at": 9999999999}
        _save_tokens(tokens)

        token_file = tmp_path / "strava_tokens.json"
        assert token_file.exists()
        assert json.loads(token_file.read_text()) == tokens

    def test_save_tokens_sets_chmod_600(self, tmp_path, monkeypatch):
        token_file = tmp_path / "strava_tokens.json"
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        _save_tokens({"access_token": "x"})

        mode = oct(token_file.stat().st_mode)[-3:]
        assert mode == "600"

    def test_load_tokens_missing_returns_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", tmp_path / "nonexistent.json")
        assert _load_tokens() is None

    def test_load_tokens_valid_json(self, tmp_path, monkeypatch):
        token_file = tmp_path / "strava_tokens.json"
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
        token_file = tmp_path / "strava_tokens.json"
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        future_expires_at = int(time.time()) + 21600
        payload = {
            "access_token": "new_acc",
            "refresh_token": "new_ref",
            "expires_at": future_expires_at,
        }
        mock_resp = _mock_post_response(payload)

        with patch(f"{MODULE}.requests.post", return_value=mock_resp):
            result = _refresh_tokens({"refresh_token": "old_ref"}, "sid", "ssec")

        assert result["access_token"] == "new_acc"
        assert result["refresh_token"] == "new_ref"
        assert result["expires_at"] == future_expires_at

    def test_refresh_failure_raises_http_error(self, monkeypatch):
        mock_resp = _mock_post_response({}, status_code=401)

        with patch(f"{MODULE}.requests.post", return_value=mock_resp):
            with pytest.raises(requests.HTTPError):
                _refresh_tokens({"refresh_token": "old"}, "sid", "ssec")

    def test_refresh_uses_expires_at_from_response(self, tmp_path, monkeypatch):
        """Strava returns expires_at directly — must use it, not compute it."""
        token_file = tmp_path / "strava_tokens.json"
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        exact_expires_at = 9_999_999_999
        payload = {
            "access_token": "a",
            "refresh_token": "b",
            "expires_at": exact_expires_at,
        }
        mock_resp = _mock_post_response(payload)

        with patch(f"{MODULE}.requests.post", return_value=mock_resp):
            result = _refresh_tokens({"refresh_token": "old"}, "sid", "ssec")

        assert result["expires_at"] == exact_expires_at

    def test_default_expiry_21600(self, tmp_path, monkeypatch):
        """When expires_at is absent from response, fallback must be 21600 s."""
        token_file = tmp_path / "strava_tokens.json"
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        # Response has no expires_at
        payload = {"access_token": "a", "refresh_token": "b"}
        mock_resp = _mock_post_response(payload)

        with patch(f"{MODULE}.requests.post", return_value=mock_resp):
            result = _refresh_tokens({"refresh_token": "old"}, "sid", "ssec")

        assert result["expires_at"] == pytest.approx(time.time() + 21600, abs=5)

    def test_refresh_does_not_include_redirect_uri(self, tmp_path, monkeypatch):
        """Strava token refresh POST must NOT include redirect_uri."""
        token_file = tmp_path / "strava_tokens.json"
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        payload = {"access_token": "a", "refresh_token": "b", "expires_at": 9999999999}
        mock_resp = _mock_post_response(payload)

        with patch(f"{MODULE}.requests.post", return_value=mock_resp) as mock_post:
            _refresh_tokens({"refresh_token": "old"}, "sid", "ssec")

        call_data = mock_post.call_args[1].get("data", {})
        assert "redirect_uri" not in call_data


# ---------------------------------------------------------------------------
# get_access_token
# ---------------------------------------------------------------------------


class TestGetAccessToken:
    def test_get_access_token_cached(self, tmp_path, monkeypatch):
        """Valid cached token must be returned without any HTTP call."""
        tokens = _make_valid_tokens(offset=7200)
        token_file = tmp_path / "strava_tokens.json"
        token_file.write_text(json.dumps(tokens))
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        with (
            patch(f"{MODULE}.get_secret") as mock_secret,
            patch(f"{MODULE}.requests.post") as mock_post,
        ):
            mock_secret.side_effect = lambda k, **_: (
                "sid" if k == "STRAVA_CLIENT_ID" else "ssec"
            )
            result = get_access_token()

        assert result == "acc_strava"
        mock_post.assert_not_called()

    def test_get_access_token_expired_refreshes(self, tmp_path, monkeypatch):
        """Expired token must trigger _refresh_tokens and return new token."""
        expired = _make_valid_tokens(offset=-100)
        token_file = tmp_path / "strava_tokens.json"
        token_file.write_text(json.dumps(expired))
        monkeypatch.setattr(f"{MODULE}.TOKEN_FILE", token_file)
        monkeypatch.setattr(f"{MODULE}.CONFIG_DIR", tmp_path)

        future_expires_at = int(time.time()) + 21600
        refresh_payload = {
            "access_token": "refreshed_acc",
            "refresh_token": "refreshed_ref",
            "expires_at": future_expires_at,
        }
        mock_resp = _mock_post_response(refresh_payload)

        with (
            patch(f"{MODULE}.get_secret") as mock_secret,
            patch(f"{MODULE}.requests.post", return_value=mock_resp),
        ):
            mock_secret.side_effect = lambda k, **_: (
                "sid" if k == "STRAVA_CLIENT_ID" else "ssec"
            )
            result = get_access_token()

        assert result == "refreshed_acc"
