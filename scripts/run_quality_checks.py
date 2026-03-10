"""
run_quality_checks.py — CLI entrypoint for data quality checks.

Usage:
    python scripts/run_quality_checks.py                      # all checks
    python scripts/run_quality_checks.py --table daily_sleep   # single table
    python scripts/run_quality_checks.py --type freshness      # single check type
    python scripts/run_quality_checks.py --json                # JSON output
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Add platform root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))

import duckdb
from health_platform.quality.data_quality_checker import DataQualityChecker
from health_platform.quality.models import QualityReport
from health_platform.quality.reporters import format_for_log
from health_platform.utils.logging_config import get_logger

logger = get_logger("run_quality_checks")


def get_db_path() -> str:
    """Resolve DuckDB path from environment."""
    if path := os.environ.get("HEALTH_DB_PATH"):
        return path
    from health_platform.utils.paths import get_db_path as _get_db_path

    return str(_get_db_path())


def main() -> None:
    parser = argparse.ArgumentParser(description="Run data quality checks")
    parser.add_argument("--table", help="Check a single table")
    parser.add_argument("--type", dest="check_type", help="Run a single check type")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    args = parser.parse_args()

    db_path = get_db_path()
    logger.info("Running DQ checks against: %s", db_path)

    con = duckdb.connect(db_path, read_only=True)
    try:
        checker = DataQualityChecker(con)

        if args.table:
            results = checker.run_table_checks(args.table)
            report = QualityReport(results=results)
            report.finish()
        elif args.check_type:
            results = checker.run_check_type(args.check_type)
            report = QualityReport(results=results)
            report.finish()
        else:
            report = checker.run_all_checks()
    finally:
        con.close()

    if args.json:
        output = {
            "total": report.total,
            "passed": report.passed,
            "failed": report.failed,
            "pass_rate": report.pass_rate,
            "results": [
                {
                    "table": r.table_name,
                    "check_type": r.check_type,
                    "column": r.column,
                    "passed": r.passed,
                    "message": r.message,
                }
                for r in report.results
            ],
        }
        print(json.dumps(output, indent=2))
    else:
        print(format_for_log(report))

    sys.exit(0 if report.failed == 0 else 1)


if __name__ == "__main__":
    main()
