"""Canonical SQL validation utilities.

Single source of truth for SQL injection prevention across the platform.
All modules that validate SQL identifiers or clauses should import from here.
"""

from __future__ import annotations

import re

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*\Z")

# Dangerous SQL keywords that should never appear in a WHERE clause
# sourced from config files.
_DANGEROUS_KEYWORDS = re.compile(
    r"\b(UNION|DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|EXEC|EXECUTE|TRUNCATE|GRANT|REVOKE|LOAD|INSTALL|COPY|ATTACH)\b",
    re.IGNORECASE,
)

# Patterns that indicate injection attempts
_DANGEROUS_PATTERNS = re.compile(
    r"(--|/\*|\*/|;)",
)


def is_safe_identifier(name: str) -> bool:
    """Check if name is a safe SQL identifier. Returns bool, does not raise."""
    return bool(_SAFE_IDENTIFIER.match(name))


def validate_sql_identifier(name: str) -> str:
    """Validate that name is a safe SQL identifier. Raises ValueError if not."""
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def validate_where_clause(clause: str) -> str:
    """Validate that a WHERE clause fragment contains only safe SQL components.

    Intended for defense-in-depth on config-sourced WHERE clauses (e.g. from
    quality_rules.yaml). Rejects clauses containing dangerous keywords or
    injection patterns.

    Parameters
    ----------
    clause : str
        The WHERE clause fragment to validate.

    Returns
    -------
    str
        The clause unchanged, if valid.

    Raises
    ------
    ValueError
        If the clause contains dangerous keywords or patterns.
    """
    if not clause or not clause.strip():
        raise ValueError("WHERE clause must not be empty")

    if _DANGEROUS_PATTERNS.search(clause):
        raise ValueError(
            f"WHERE clause contains dangerous pattern (;, --, or block comment): {clause!r}"
        )

    match = _DANGEROUS_KEYWORDS.search(clause)
    if match:
        raise ValueError(
            f"WHERE clause contains dangerous keyword '{match.group()}': {clause!r}"
        )

    return clause
