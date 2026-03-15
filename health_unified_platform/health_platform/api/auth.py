"""Bearer token authentication for the Health API.

Loads the API token from macOS Keychain (claude.keychain-db) via `security` CLI.
Falls back to HEALTH_API_TOKEN environment variable.

Setup:
    security add-generic-password -a claude -s HEALTH_API_TOKEN \
        -w "<your-token>" ~/Library/Keychains/claude.keychain-db
"""

from __future__ import annotations

import os
import secrets

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from health_platform.utils.keychain import get_secret
from health_platform.utils.logging_config import get_logger

logger = get_logger("api.auth")

_bearer_scheme = HTTPBearer(auto_error=False)

_cached_token: str | None = None


def get_api_token() -> str:
    """Resolve the API bearer token from Keychain or environment.

    Priority:
    1. claude.keychain-db (HEALTH_API_TOKEN)
    2. HEALTH_API_TOKEN environment variable
    3. Auto-generate and warn (dev only — never in production)
    """
    global _cached_token
    if _cached_token is not None:
        return _cached_token

    token = get_secret("HEALTH_API_TOKEN")
    if token:
        _cached_token = token
        return token

    # Only allow ephemeral tokens in dev
    env = os.environ.get("HEALTH_ENV", "dev")
    if env != "dev":
        raise RuntimeError(
            "HEALTH_API_TOKEN not configured. "
            "Store it: security add-generic-password -a claude "
            "-s HEALTH_API_TOKEN -w '<token>' ~/Library/Keychains/claude.keychain-db"
        )

    token = secrets.token_urlsafe(32)
    logger.debug(
        "DEV MODE: No API token configured. Generated ephemeral token (first 8 chars redacted).",
    )
    _cached_token = token
    return token


async def verify_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> str:
    """FastAPI dependency that verifies the Bearer token.

    Accepts both standard ``Authorization: Bearer <token>`` and the custom
    ``Bearer: <token>`` header sent by Health Auto Export (HAE) iOS app.
    """
    token: str | None = None
    if credentials is not None:
        token = credentials.credentials
    else:
        # Fallback: HAE sends token as custom "Bearer" header
        token = request.headers.get("Bearer")

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
        )

    expected = get_api_token()
    if not secrets.compare_digest(token, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API token",
        )
    return token
