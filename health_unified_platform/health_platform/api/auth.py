"""Bearer token authentication for the Health API.

Loads the API token from macOS Keychain via `security` CLI.
Falls back to HEALTH_API_TOKEN environment variable.

Setup:
    security add-generic-password -a health-api -s health-api-token -w "<your-token>"
"""

from __future__ import annotations

import os
import secrets
import subprocess

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from health_platform.utils.logging_config import get_logger

logger = get_logger("api.auth")

_bearer_scheme = HTTPBearer()

_cached_token: str | None = None


def _load_token_from_keychain() -> str | None:
    """Load API token from macOS Keychain."""
    try:
        result = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-a",
                "health-api",
                "-s",
                "health-api-token",
                "-w",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            token = result.stdout.strip()
            if token:
                logger.info("API token loaded from macOS Keychain")
                return token
    except Exception as exc:
        logger.debug("Keychain lookup failed: %s", exc)
    return None


def get_api_token() -> str:
    """Resolve the API bearer token from Keychain or environment.

    Priority:
    1. macOS Keychain (health-api-token)
    2. HEALTH_API_TOKEN environment variable
    3. Auto-generate and warn (dev only)
    """
    global _cached_token
    if _cached_token is not None:
        return _cached_token

    token = _load_token_from_keychain()
    if token:
        _cached_token = token
        return token

    token = os.environ.get("HEALTH_API_TOKEN")
    if token:
        logger.info("API token loaded from HEALTH_API_TOKEN env var")
        _cached_token = token
        return token

    # Dev fallback: auto-generate a token and log it
    token = secrets.token_urlsafe(32)
    logger.warning(
        "No API token configured. Generated ephemeral token for this session. "
        "Set up Keychain or HEALTH_API_TOKEN for production use."
    )
    _cached_token = token
    return token


async def verify_token(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> str:
    """FastAPI dependency that verifies the Bearer token."""
    expected = get_api_token()
    if not secrets.compare_digest(credentials.credentials, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API token",
        )
    return credentials.credentials
