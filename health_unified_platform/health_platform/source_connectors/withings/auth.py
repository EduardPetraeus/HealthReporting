"""
OAuth 2.0 Authorization Code flow for the Withings API.
Handles first-time browser auth, token storage and automatic refresh.

Withings API reference: https://developer.withings.com/api-reference
"""

from __future__ import annotations

import json
import os
import secrets
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import requests
from health_platform.utils.keychain import get_secret
from health_platform.utils.logging_config import get_logger

logger = get_logger("withings.auth")

CONFIG_DIR = Path.home() / ".config" / "health_reporting"
TOKEN_FILE = CONFIG_DIR / "withings_tokens.json"

AUTHORIZE_URL = "https://account.withings.com/oauth2_user/authorize2"
TOKEN_URL = "https://wbsapi.withings.net/v2/oauth2"
CALLBACK_PORT = 8081
OAUTH_SCOPES = "user.info,user.metrics,user.activity"


def _load_credentials() -> tuple[str, str, str]:
    """Load OAuth credentials from macOS Keychain (claude.keychain-db).

    Setup::

        security add-generic-password -a claude -s WITHINGS_CLIENT_ID \
            -w "<id>" ~/Library/Keychains/claude.keychain-db
        security add-generic-password -a claude -s WITHINGS_CLIENT_SECRET \
            -w "<secret>" ~/Library/Keychains/claude.keychain-db
    """
    client_id = get_secret("WITHINGS_CLIENT_ID", fallback_env=False)
    client_secret = get_secret("WITHINGS_CLIENT_SECRET", fallback_env=False)
    redirect_uri = (
        os.environ.get("WITHINGS_REDIRECT_URI")
        or f"http://localhost:{CALLBACK_PORT}/callback"
    )
    if not client_id or not client_secret:
        raise ValueError(
            "Missing WITHINGS_CLIENT_ID or WITHINGS_CLIENT_SECRET. "
            "Store them in macOS Keychain:\n"
            "  security add-generic-password -a claude -s WITHINGS_CLIENT_ID "
            "-w '<id>' ~/Library/Keychains/claude.keychain-db\n"
            "  security add-generic-password -a claude -s WITHINGS_CLIENT_SECRET "
            "-w '<secret>' ~/Library/Keychains/claude.keychain-db"
        )
    return client_id, client_secret, redirect_uri


def _save_tokens(tokens: dict) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(json.dumps(tokens, indent=2))
    TOKEN_FILE.chmod(0o600)


def _load_tokens() -> dict | None:
    if TOKEN_FILE.exists():
        return json.loads(TOKEN_FILE.read_text())
    return None


def _is_expired(tokens: dict) -> bool:
    """Returns True if the access token expires within 60 seconds."""
    return time.time() >= tokens.get("expires_at", 0) - 60


def _refresh_tokens(tokens: dict, client_id: str, client_secret: str) -> dict:
    """Refresh the access token using the refresh token.

    Withings uses action-based token endpoint (action=requesttoken).
    """
    logger.info("Refreshing Withings access token...")
    response = requests.post(
        TOKEN_URL,
        data={
            "action": "requesttoken",
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": tokens["refresh_token"],
        },
    )
    response.raise_for_status()
    body = response.json().get("body", response.json())
    body["expires_at"] = time.time() + body.get("expires_in", 10800)
    _save_tokens(body)
    logger.info("Token refreshed successfully.")
    return body


class _CallbackHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler that captures the OAuth authorization code."""

    def do_GET(self) -> None:
        params = parse_qs(urlparse(self.path).query)
        if "code" in params:
            received_state = params.get("state", [None])[0]
            expected_state = getattr(self.server, "expected_state", None)
            if received_state != expected_state:
                self.send_response(403)
                self.end_headers()
                self.wfile.write(b"Invalid state parameter (CSRF check failed).")
                return
            self.server.auth_code = params["code"][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Authorization successful. You can close this tab.")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing authorization code.")

    def log_message(self, format: str, *args) -> None:  # noqa: A002
        pass  # suppress request logs


def _run_auth_flow(client_id: str, client_secret: str, redirect_uri: str) -> dict:
    """Opens the browser for OAuth login and exchanges the code for tokens."""
    state = secrets.token_urlsafe(16)
    auth_url = f"{AUTHORIZE_URL}?" + urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": OAUTH_SCOPES,
            "state": state,
        }
    )
    logger.info("Opening browser for Withings authorization...")
    webbrowser.open(auth_url)

    logger.info("Waiting for OAuth callback on port %d...", CALLBACK_PORT)
    server = HTTPServer(("localhost", CALLBACK_PORT), _CallbackHandler)
    server.expected_state = state
    server.auth_code = None
    server.handle_request()

    code = server.auth_code
    if not code:
        raise RuntimeError("No authorization code received in callback.")

    response = requests.post(
        TOKEN_URL,
        data={
            "action": "requesttoken",
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    response.raise_for_status()
    body = response.json().get("body", response.json())
    body["expires_at"] = time.time() + body.get("expires_in", 10800)
    _save_tokens(body)
    logger.info("Authorization successful. Tokens saved to %s", TOKEN_FILE)
    return body


def get_access_token() -> str:
    """
    Returns a valid Withings access token.
    Runs the browser OAuth flow on first use.
    Auto-refreshes when the token is near expiry.
    """
    client_id, client_secret, redirect_uri = _load_credentials()
    tokens = _load_tokens()

    if tokens is None:
        tokens = _run_auth_flow(client_id, client_secret, redirect_uri)
    elif _is_expired(tokens):
        tokens = _refresh_tokens(tokens, client_id, client_secret)

    return tokens["access_token"]


def build_authorize_url() -> str:
    """Build the OAuth authorization URL for testing or manual flows."""
    client_id, _, redirect_uri = _load_credentials()
    state = secrets.token_urlsafe(16)
    return f"{AUTHORIZE_URL}?" + urlencode(
        {
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": OAUTH_SCOPES,
            "state": state,
        }
    )
