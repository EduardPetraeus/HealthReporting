"""Shared macOS Keychain utility for secrets management.

Reads secrets from ~/Library/Keychains/claude.keychain-db using the
macOS `security` CLI. Falls back to environment variables when keychain
is unavailable (CI, Linux, etc.).

Usage:
    from health_platform.utils.keychain import get_secret

    api_key = get_secret("ANTHROPIC_API_KEY")
    client_id = get_secret("OURA_CLIENT_ID", fallback_env=False)
"""

from __future__ import annotations

import os
import subprocess

from health_platform.utils.logging_config import get_logger

logger = get_logger("utils.keychain")

KEYCHAIN_PATH = os.path.expanduser("~/Library/Keychains/claude.keychain-db")


def get_secret(key_name: str, *, fallback_env: bool = True) -> str | None:
    """Read a secret from macOS Keychain, with optional env var fallback.

    Args:
        key_name: The service name in the keychain (e.g. "ANTHROPIC_API_KEY").
        fallback_env: If True, falls back to os.environ.get(key_name) when
            keychain lookup fails. Defaults to True.

    Returns:
        The secret string, or None if not found anywhere.
    """
    # Try macOS Keychain first
    try:
        result = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-a",
                "claude",
                "-s",
                key_name,
                "-w",
                KEYCHAIN_PATH,
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            value = result.stdout.strip()
            if value:
                logger.debug("Loaded %s from keychain", key_name)
                return value
    except Exception as exc:
        logger.debug("Keychain lookup for %s failed: %s", key_name, exc)

    # Fallback to environment variable
    if fallback_env:
        value = os.environ.get(key_name)
        if value:
            logger.debug("Loaded %s from environment", key_name)
            return value

    return None
