"""Canonical SQL validation utilities.

Single source of truth for SQL injection prevention across the platform.
All modules that validate SQL identifiers or clauses should import from here.
"""

from __future__ import annotations

import re

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*\Z")

# Whitelist of SQL keywords allowed in WHERE clauses
_ALLOWED_WHERE_KEYWORDS = frozenset(
    {
        "AND",
        "OR",
        "NOT",
        "IS",
        "NULL",
        "LIKE",
        "IN",
        "BETWEEN",
        "TRUE",
        "FALSE",
        "CASE",
        "WHEN",
        "THEN",
        "ELSE",
        "END",
        "CAST",
        "AS",
        "DATE",
        "TIMESTAMP",
        "INTEGER",
        "VARCHAR",
        "FLOAT",
        "DOUBLE",
        "BOOLEAN",
    }
)

# Explicitly denied SQL keywords — blocked even though they look like identifiers
_DENIED_WHERE_KEYWORDS = frozenset(
    {
        "SELECT",
        "UNION",
        "DROP",
        "DELETE",
        "INSERT",
        "UPDATE",
        "ALTER",
        "CREATE",
        "EXEC",
        "EXECUTE",
        "TRUNCATE",
        "GRANT",
        "REVOKE",
        "LOAD",
        "INSTALL",
        "COPY",
        "ATTACH",
        "SLEEP",
        "BENCHMARK",
        "WAITFOR",
        "PRAGMA",
        "CALL",
        "FROM",
        "INTO",
        "TABLE",
        "SET",
        "ON",
        "TO",
        "ALL",
        "ADD",
    }
)

# Tokenizer for whitelist-based WHERE clause validation
_WHERE_TOKEN = re.compile(
    r"""
    \s+                             |  # whitespace
    '(?:[^'\\]|\\.)*'               |  # single-quoted string literal
    -?[0-9]+(?:\.[0-9]+)?           |  # numeric literal
    [a-zA-Z_][a-zA-Z0-9_]*         |  # identifier or keyword
    \.                              |  # dot (table.column separator)
    [<>!=]{1,2}                     |  # comparison operators
    [(),]                           |  # parentheses, comma
    %                                  # LIKE wildcard
    """,
    re.VERBOSE,
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
    """Validate a WHERE clause using whitelist-based token grammar.

    Only allows safe SQL constructs: identifiers, comparison operators,
    logical operators, literals, and a curated set of SQL keywords.
    Blocks all other keywords (UNION, DROP, SELECT, SLEEP, etc.).

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
        If the clause contains disallowed tokens or keywords.
    """
    if not clause or not clause.strip():
        raise ValueError("WHERE clause must not be empty")

    pos = 0
    text = clause.strip()
    while pos < len(text):
        match = _WHERE_TOKEN.match(text, pos)
        if not match:
            raise ValueError(
                f"WHERE clause contains disallowed token at position {pos}: "
                f"{text[pos : pos + 20]!r}"
            )
        token = match.group().strip()
        if token and re.match(r"^[a-zA-Z_]", token):
            upper = token.upper()
            if upper in _DENIED_WHERE_KEYWORDS:
                raise ValueError(f"WHERE clause contains disallowed keyword: {token!r}")
            if upper not in _ALLOWED_WHERE_KEYWORDS:
                if not _SAFE_IDENTIFIER.match(token):
                    raise ValueError(
                        f"WHERE clause contains invalid identifier: {token!r}"
                    )
        pos = match.end()

    return clause
