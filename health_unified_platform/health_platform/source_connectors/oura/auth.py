"""
OAuth 2.0 Authorization Code flow for the Oura API.
Handles first-time browser auth, token storage and automatic refresh.
"""

from __future__ import annotations

import json
import os
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

import requests
from dotenv import load_dotenv

CONFIG_DIR = Path.home() / ".config" / "health_reporting"
ENV_FILE = CONFIG_DIR / ".env"
TOKEN_FILE = CONFIG_DIR / "oura_tokens.json"

AUTHORIZE_URL = "https://cloud.ouraring.com/oauth/authorize"
TOKEN_URL = "https://api.ouraring.com/oauth/token"
CALLBACK_PORT = 8080
OAUTH_SCOPES = (
    "email personal daily heartrate tag workout session "
    "spo2 ring_configuration stress heart_health"
)


def _load_credentials() -> tuple[str, str, str]:
    load_dotenv(ENV_FILE)
    client_id = os.getenv("OURA_CLIENT_ID")
    client_secret = os.getenv("OURA_CLIENT_SECRET")
    redirect_uri = os.getenv("OURA_REDIRECT_URI", f"http://localhost:{CALLBACK_PORT}/callback")
    if not client_id or not client_secret:
        raise ValueError(f"Missing OURA_CLIENT_ID or OURA_CLIENT_SECRET in {ENV_FILE}")
    return client_id, client_secret, redirect_uri


def _save_tokens(tokens: dict) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(json.dumps(tokens, indent=2))


def _load_tokens() -> dict | None:
    if TOKEN_FILE.exists():
        return json.loads(TOKEN_FILE.read_text())
    return None


def _is_expired(tokens: dict) -> bool:
    """Returns True if the access token expires within 60 seconds."""
    return time.time() >= tokens.get("expires_at", 0) - 60


def _refresh_tokens(tokens: dict, client_id: str, client_secret: str) -> dict:
    print("Refreshing Oura access token...")
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": tokens["refresh_token"],
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    response.raise_for_status()
    new_tokens = response.json()
    new_tokens["expires_at"] = time.time() + new_tokens.get("expires_in", 86400)
    _save_tokens(new_tokens)
    print("Token refreshed successfully.")
    return new_tokens


class _CallbackHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler that captures the OAuth authorization code."""

    auth_code: str | None = None

    def do_GET(self) -> None:
        params = parse_qs(urlparse(self.path).query)
        if "code" in params:
            _CallbackHandler.auth_code = params["code"][0]
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
    auth_url = (
        f"{AUTHORIZE_URL}?"
        + urlencode({
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "scope": OAUTH_SCOPES,
        })
    )
    print(f"Opening browser for Oura authorization...")
    webbrowser.open(auth_url)

    print(f"Waiting for OAuth callback on port {CALLBACK_PORT}...")
    server = HTTPServer(("localhost", CALLBACK_PORT), _CallbackHandler)
    server.handle_request()

    code = _CallbackHandler.auth_code
    if not code:
        raise RuntimeError("No authorization code received in callback.")

    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "client_secret": client_secret,
        },
    )
    response.raise_for_status()
    tokens = response.json()
    tokens["expires_at"] = time.time() + tokens.get("expires_in", 86400)
    _save_tokens(tokens)
    print(f"Authorization successful. Tokens saved to {TOKEN_FILE}")
    return tokens


def get_access_token() -> str:
    """
    Returns a valid Oura access token.
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
