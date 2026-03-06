"""Bearer token authentication for the Health API.

Loads the API token from macOS Keychain (claude.keychain-db) via `security` CLI.
Falls back to HEALTH_API_TOKEN environment variable.

Setup:
    security add-generic-password -a claude -s HEALTH_API_TOKEN -w "<your-token>" ~/Library/Keychains/claude.keychain-db
"""

from __future__ import annotations

import secrets

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from health_platform.utils.keychain import get_secret
from health_platform.utils.logging_config import get_logger

logger = get_logger("api.auth")

_bearer_scheme = HTTPBearer()

_cached_token: str | None = None


def get_api_token() -> str:
    """Resolve the API bearer token from Keychain or environment.

    Priority:
    1. claude.keychain-db (HEALTH_API_TOKEN)
    2. HEALTH_API_TOKEN environment variable
    3. Auto-generate and warn (dev only)
    """
    global _cached_token
    if _cached_token is not None:
        return _cached_token

    token = get_secret("HEALTH_API_TOKEN")
    if token:
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
