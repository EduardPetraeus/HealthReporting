"""Load and validate data quality rules from YAML configuration."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from health_platform.utils.logging_config import get_logger

logger = get_logger("rule_loader")

_VALID_CHECK_TYPES = {"not_null", "unique", "freshness", "row_count", "value_range"}
_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*\Z")

_DEFAULT_RULES_PATH = (
    Path(__file__).resolve().parents[1]
    / ".."
    / "health_environment"
    / "config"
    / "quality_rules.yaml"
)


def load_rules(rules_path: Path | None = None) -> dict[str, Any]:
    """Load quality rules from YAML file.

    Parameters
    ----------
    rules_path : Path, optional
        Path to quality_rules.yaml. Defaults to the standard config location.

    Returns
    -------
    dict
        Parsed rules keyed by table name.

    Raises
    ------
    FileNotFoundError
        If the rules file does not exist.
    ValueError
        If the YAML is invalid or contains unknown check types.
    """
    path = rules_path or _DEFAULT_RULES_PATH.resolve()
    if not path.exists():
        raise FileNotFoundError(f"Quality rules file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not data or "tables" not in data:
        raise ValueError(f"Quality rules file must have a 'tables' key: {path}")

    tables = data["tables"]
    _validate_rules(tables)
    logger.debug("Loaded quality rules for %d tables", len(tables))
    return tables


def _validate_rules(tables: dict[str, Any]) -> None:
    """Validate table names, check types, and value schemas at load time."""
    for table_name, checks in tables.items():
        if not _SAFE_IDENTIFIER.match(table_name):
            raise ValueError(
                f"Invalid table name '{table_name}' -- "
                f"must match [a-zA-Z_][a-zA-Z0-9_]*"
            )
        if not isinstance(checks, dict):
            raise ValueError(
                f"Table '{table_name}' rules must be a dict, "
                f"got {type(checks).__name__}"
            )
        for check_type, check_value in checks.items():
            if check_type not in _VALID_CHECK_TYPES:
                raise ValueError(
                    f"Unknown check type '{check_type}' for table '{table_name}'. "
                    f"Valid types: {', '.join(sorted(_VALID_CHECK_TYPES))}"
                )
            _validate_check_schema(table_name, check_type, check_value)


def _validate_check_schema(table_name: str, check_type: str, check_value: Any) -> None:
    """Validate the schema of a single check's configuration value."""
    if check_type in ("not_null", "unique"):
        if not isinstance(check_value, list) or not all(
            isinstance(c, str) for c in check_value
        ):
            raise ValueError(
                f"Table '{table_name}'.{check_type} must be a list of column names"
            )
        for col in check_value:
            if not _SAFE_IDENTIFIER.match(col):
                raise ValueError(f"Invalid column '{col}' in {table_name}.{check_type}")

    elif check_type == "freshness":
        if not isinstance(check_value, dict):
            raise ValueError(
                f"Table '{table_name}'.freshness must be a dict "
                f"with 'column' and 'max_hours'"
            )
        if "column" not in check_value or "max_hours" not in check_value:
            raise ValueError(
                f"Table '{table_name}'.freshness requires "
                f"'column' and 'max_hours' keys"
            )
        if not isinstance(check_value["max_hours"], (int, float)):
            raise ValueError(
                f"Table '{table_name}'.freshness.max_hours must be numeric, "
                f"got {type(check_value['max_hours']).__name__}"
            )
        if not _SAFE_IDENTIFIER.match(check_value["column"]):
            raise ValueError(
                f"Invalid column '{check_value['column']}' "
                f"in {table_name}.freshness"
            )

    elif check_type == "row_count":
        if not isinstance(check_value, dict) or "min_rows" not in check_value:
            raise ValueError(
                f"Table '{table_name}'.row_count must be a dict with 'min_rows' key"
            )
        if not isinstance(check_value["min_rows"], (int, float)):
            raise ValueError(
                f"Table '{table_name}'.row_count.min_rows must be numeric, "
                f"got {type(check_value['min_rows']).__name__}"
            )

    elif check_type == "value_range":
        if not isinstance(check_value, dict):
            raise ValueError(
                f"Table '{table_name}'.value_range must be a dict of column ranges"
            )
        for col, bounds in check_value.items():
            if not _SAFE_IDENTIFIER.match(col):
                raise ValueError(f"Invalid column '{col}' in {table_name}.value_range")
            if not isinstance(bounds, dict):
                raise ValueError(
                    f"Table '{table_name}'.value_range.{col} "
                    f"must be a dict with min/max"
                )
            for key in ("min", "max"):
                if key in bounds and not isinstance(bounds[key], (int, float)):
                    raise ValueError(
                        f"Table '{table_name}'.value_range.{col}.{key} "
                        f"must be numeric, got {type(bounds[key]).__name__}"
                    )


def list_tables(rules_path: Path | None = None) -> list[str]:
    """Return sorted list of table names from the rules file."""
    tables = load_rules(rules_path)
    return sorted(tables.keys())
