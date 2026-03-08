"""Core data quality checker engine.

Reads YAML rules and generates SQL checks against DuckDB.
Non-fatal: all check errors are captured as failed results, never crash the pipeline.
"""

from __future__ import annotations

import re
import warnings
from pathlib import Path
from typing import Any, Optional

import duckdb

from health_platform.quality.models import CheckResult, QualityReport
from health_platform.quality.rule_loader import load_rules
from health_platform.utils.logging_config import get_logger

logger = get_logger("data_quality_checker")

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*\Z")


def _validate_id(name: str) -> str:
    """Validate that a string is a safe SQL identifier (prevents injection)."""
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


class DataQualityChecker:
    """Run data quality checks against a DuckDB connection using YAML rules."""

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        rules_path: Path | None = None,
    ) -> None:
        self.con = con
        self._rules = load_rules(rules_path)

    def run_all_checks(self) -> QualityReport:
        """Run all quality checks for all tables. Returns a QualityReport."""
        report = QualityReport()
        for table_name in sorted(self._rules.keys()):
            results = self.run_table_checks(table_name)
            report.results.extend(results)
        report.finish()
        return report

    def run_table_checks(self, table_name: str) -> list[CheckResult]:
        """Run all checks for a single table."""
        if table_name not in self._rules:
            return [
                CheckResult(
                    table_name=table_name,
                    check_type="config",
                    column=None,
                    passed=False,
                    message=f"No rules defined for table '{table_name}'",
                )
            ]

        checks = self._rules[table_name]
        results: list[CheckResult] = []

        if not self._table_exists(table_name):
            results.append(
                CheckResult(
                    table_name=table_name,
                    check_type="existence",
                    column=None,
                    passed=False,
                    message=f"Table 'silver.{table_name}' does not exist",
                )
            )
            return results

        for check_type, config in checks.items():
            try:
                check_results = self._dispatch_check(table_name, check_type, config)
                results.extend(check_results)
            except Exception as exc:
                warnings.warn(
                    f"[DQ] Check {check_type} on {table_name} failed (non-fatal): {exc}",
                    stacklevel=2,
                )
                results.append(
                    CheckResult(
                        table_name=table_name,
                        check_type=check_type,
                        column=None,
                        passed=False,
                        message=f"Check error: {exc}",
                    )
                )

        return results

    def run_check_type(self, check_type: str) -> list[CheckResult]:
        """Run a single check type across all tables that define it."""
        results: list[CheckResult] = []
        for table_name, checks in sorted(self._rules.items()):
            if check_type in checks:
                if not self._table_exists(table_name):
                    results.append(
                        CheckResult(
                            table_name=table_name,
                            check_type="existence",
                            column=None,
                            passed=False,
                            message=f"Table 'silver.{table_name}' does not exist",
                        )
                    )
                    continue
                try:
                    check_results = self._dispatch_check(
                        table_name, check_type, checks[check_type]
                    )
                    results.extend(check_results)
                except Exception as exc:
                    warnings.warn(
                        f"[DQ] Check {check_type} on {table_name} failed: {exc}",
                        stacklevel=2,
                    )
                    results.append(
                        CheckResult(
                            table_name=table_name,
                            check_type=check_type,
                            column=None,
                            passed=False,
                            message=f"Check error: {exc}",
                        )
                    )
        return results

    # ------------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------------

    def _dispatch_check(
        self, table_name: str, check_type: str, config: Any
    ) -> list[CheckResult]:
        handlers = {
            "not_null": self._check_not_null,
            "unique": self._check_unique,
            "freshness": self._check_freshness,
            "row_count": self._check_row_count,
            "value_range": self._check_value_range,
            "schema_drift": self._check_schema_drift,
        }
        handler = handlers.get(check_type)
        if not handler:
            return [
                CheckResult(
                    table_name=table_name,
                    check_type=check_type,
                    column=None,
                    passed=False,
                    message=f"Unknown check type '{check_type}'",
                )
            ]
        return handler(table_name, config)

    # ------------------------------------------------------------------
    # Check implementations
    # ------------------------------------------------------------------

    def _check_not_null(self, table_name: str, columns: list[str]) -> list[CheckResult]:
        results: list[CheckResult] = []
        tbl = _validate_id(table_name)
        for col in columns:
            c = _validate_id(col)
            sql = f"SELECT COUNT(*) FROM silver.{tbl} WHERE {c} IS NULL"
            null_count = self._query_scalar(sql) or 0
            passed = null_count == 0
            results.append(
                CheckResult(
                    table_name=table_name,
                    check_type="not_null",
                    column=col,
                    passed=passed,
                    message=(
                        f"{col}: OK" if passed else f"{col}: {null_count} null row(s)"
                    ),
                    value=float(null_count),
                    threshold=0.0,
                )
            )
        return results

    def _check_unique(self, table_name: str, columns: list[str]) -> list[CheckResult]:
        results: list[CheckResult] = []
        tbl = _validate_id(table_name)
        for col in columns:
            c = _validate_id(col)
            sql = (
                f"SELECT COUNT(*) - COUNT(DISTINCT {c}) "
                f"FROM silver.{tbl} "
                f"WHERE {c} IS NOT NULL"
            )
            dup_count = self._query_scalar(sql) or 0
            passed = dup_count == 0
            results.append(
                CheckResult(
                    table_name=table_name,
                    check_type="unique",
                    column=col,
                    passed=passed,
                    message=(
                        f"{col}: unique"
                        if passed
                        else f"{col}: {dup_count} duplicate(s)"
                    ),
                    value=float(dup_count),
                    threshold=0.0,
                )
            )
        return results

    def _check_freshness(self, table_name: str, config: dict) -> list[CheckResult]:
        column = _validate_id(config.get("column", "load_datetime"))
        max_hours = config.get("max_hours", 25)
        tbl = _validate_id(table_name)
        sql = (
            f"SELECT DATEDIFF('hour', MAX({column}), CURRENT_TIMESTAMP) "
            f"FROM silver.{tbl}"
        )
        hours_stale = self._query_scalar(sql)
        if hours_stale is None:
            return [
                CheckResult(
                    table_name=table_name,
                    check_type="freshness",
                    column=column,
                    passed=False,
                    message=f"No data in {column} (table empty?)",
                )
            ]
        passed = hours_stale <= max_hours
        return [
            CheckResult(
                table_name=table_name,
                check_type="freshness",
                column=column,
                passed=passed,
                message=(
                    f"{hours_stale}h old (threshold: {max_hours}h)"
                    if passed
                    else f"STALE: {hours_stale}h old (threshold: {max_hours}h)"
                ),
                value=float(hours_stale),
                threshold=float(max_hours),
            )
        ]

    def _check_row_count(self, table_name: str, config: dict) -> list[CheckResult]:
        min_rows = config.get("min_rows", 1)
        tbl = _validate_id(table_name)
        sql = f"SELECT COUNT(*) FROM silver.{tbl}"
        row_count = self._query_scalar(sql) or 0
        passed = row_count >= min_rows
        return [
            CheckResult(
                table_name=table_name,
                check_type="row_count",
                column=None,
                passed=passed,
                message=(
                    f"{row_count} rows (min: {min_rows})"
                    if passed
                    else f"EMPTY: {row_count} rows (min: {min_rows})"
                ),
                value=float(row_count),
                threshold=float(min_rows),
            )
        ]

    def _check_value_range(
        self, table_name: str, ranges: dict[str, dict]
    ) -> list[CheckResult]:
        results: list[CheckResult] = []
        tbl = _validate_id(table_name)
        for col, bounds in ranges.items():
            c = _validate_id(col)
            min_val = bounds.get("min")
            max_val = bounds.get("max")

            conditions = []
            if min_val is not None:
                if not isinstance(min_val, (int, float)):
                    raise ValueError(f"value_range min must be numeric: {min_val!r}")
                conditions.append(f"{c} < {min_val}")
            if max_val is not None:
                if not isinstance(max_val, (int, float)):
                    raise ValueError(f"value_range max must be numeric: {max_val!r}")
                conditions.append(f"{c} > {max_val}")

            if not conditions:
                continue

            where = " OR ".join(conditions)
            sql = (
                f"SELECT COUNT(*) FROM silver.{tbl} WHERE {c} IS NOT NULL AND ({where})"
            )
            violation_count = self._query_scalar(sql) or 0
            passed = violation_count == 0
            range_str = f"[{min_val}, {max_val}]"
            results.append(
                CheckResult(
                    table_name=table_name,
                    check_type="value_range",
                    column=col,
                    passed=passed,
                    message=(
                        f"{col} within {range_str}"
                        if passed
                        else f"{col}: {violation_count} value(s) outside {range_str}"
                    ),
                    value=float(violation_count),
                    threshold=0.0,
                )
            )
        return results

    def _check_schema_drift(self, table_name: str, config: dict) -> list[CheckResult]:
        """Check that all expected columns exist in the actual table.

        Config format: { expected_columns: [col1, col2, ...] }
        The check passes if every expected column is present in the table.
        """
        expected = config.get("expected_columns", [])
        tbl = _validate_id(table_name)

        # Query actual column names from the table
        sql = (
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_schema = 'silver' AND table_name = '{tbl}'"
        )
        try:
            rows = self.con.execute(sql).fetchall()
            actual_columns = {row[0] for row in rows}
        except Exception as exc:
            return [
                CheckResult(
                    table_name=table_name,
                    check_type="schema_drift",
                    column=None,
                    passed=False,
                    message=f"Could not read schema: {exc}",
                )
            ]

        missing = sorted(set(expected) - actual_columns)
        passed = len(missing) == 0
        return [
            CheckResult(
                table_name=table_name,
                check_type="schema_drift",
                column=None,
                passed=passed,
                message=(
                    f"All {len(expected)} expected columns present"
                    if passed
                    else f"Missing columns: {', '.join(missing)}"
                ),
                value=float(len(missing)),
                threshold=0.0,
            )
        ]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _table_exists(self, table_name: str) -> bool:
        try:
            tbl = _validate_id(table_name)
            self.con.execute(f"SELECT 1 FROM silver.{tbl} LIMIT 0")
            return True
        except Exception:
            return False

    def _query_scalar(self, sql: str) -> Optional[float]:
        try:
            row = self.con.execute(sql).fetchone()
            return row[0] if row else None
        except Exception as exc:
            logger.debug("Query failed: %s — %s", sql[:100], exc)
            return None
