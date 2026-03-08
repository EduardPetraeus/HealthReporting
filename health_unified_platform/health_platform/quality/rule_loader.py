"""Load and validate data quality rules from YAML configuration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from health_platform.utils.logging_config import get_logger

logger = get_logger("rule_loader")

_VALID_CHECK_TYPES = {"not_null", "unique", "freshness", "row_count", "value_range"}

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
    """Validate that all check types in the rules are known."""
    for table_name, checks in tables.items():
        if not isinstance(checks, dict):
            raise ValueError(
                f"Table '{table_name}' rules must be a dict, got {type(checks).__name__}"
            )
        for check_type in checks:
            if check_type not in _VALID_CHECK_TYPES:
                raise ValueError(
                    f"Unknown check type '{check_type}' for table '{table_name}'. "
                    f"Valid types: {', '.join(sorted(_VALID_CHECK_TYPES))}"
                )


def list_tables(rules_path: Path | None = None) -> list[str]:
    """Return sorted list of table names from the rules file."""
    tables = load_rules(rules_path)
    return sorted(tables.keys())
