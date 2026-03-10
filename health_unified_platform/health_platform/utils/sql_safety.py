"""Canonical SQL identifier validation.

Single source of truth for SQL injection prevention across the platform.
All modules that validate SQL identifiers should import from here.
"""

from __future__ import annotations

import re

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*\Z")


def is_safe_identifier(name: str) -> bool:
    """Check if name is a safe SQL identifier. Returns bool, does not raise."""
    return bool(_SAFE_IDENTIFIER.match(name))


def validate_sql_identifier(name: str) -> str:
    """Validate that name is a safe SQL identifier. Raises ValueError if not."""
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name
