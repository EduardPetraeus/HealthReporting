"""Runner script to create gold views/tables in local DuckDB.

Reads SQL files from gold_local/ and executes them against the DuckDB database.

Usage:
    python scripts/create_gold_views.py              # Execute all gold SQL
    python scripts/create_gold_views.py --dry-run    # Print SQL without executing
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb


def get_gold_sql_dir() -> Path:
    """Return the gold_local SQL directory path."""
    return (
        Path(__file__).resolve().parent.parent
        / "health_unified_platform"
        / "health_platform"
        / "transformation_logic"
        / "gold_local"
    )


def get_sql_files(sql_dir: Path) -> list[Path]:
    """Return all SQL files from the gold_local directory, sorted.

    Dimensions are executed before facts to ensure FK targets exist.
    """
    files = sorted(sql_dir.glob("*.sql"))

    # Sort: dim_ first, then fct_, then remaining views
    dims = [f for f in files if f.stem.startswith("dim_")]
    facts = [f for f in files if f.stem.startswith("fct_")]
    views = [
        f
        for f in files
        if not f.stem.startswith("dim_") and not f.stem.startswith("fct_")
    ]

    return dims + facts + views


def run_gold_views(
    db_path: str | None = None, dry_run: bool = False
) -> dict[str, bool]:
    """Execute all gold SQL files against the DuckDB database.

    Parameters
    ----------
    db_path : str, optional
        Path to DuckDB database file. If None, resolves via get_db_path().
    dry_run : bool
        If True, print SQL without executing.

    Returns
    -------
    dict[str, bool]
        Mapping of SQL file name to success status.
    """
    sql_dir = get_gold_sql_dir()
    if not sql_dir.exists():
        print(f"ERROR: Gold SQL directory not found: {sql_dir}")
        sys.exit(1)

    sql_files = get_sql_files(sql_dir)
    if not sql_files:
        print(f"ERROR: No SQL files found in {sql_dir}")
        sys.exit(1)

    print(f"Found {len(sql_files)} gold SQL files in {sql_dir}")
    print()

    if dry_run:
        for sql_file in sql_files:
            sql = sql_file.read_text()
            print(f"-- === {sql_file.name} ===")
            print(sql)
            print()
        return {f.name: True for f in sql_files}

    # Resolve database path
    if db_path is None:
        try:
            from health_platform.utils.paths import get_db_path

            resolved = get_db_path()
            db_path = str(resolved)
        except ImportError:
            print(
                "ERROR: Could not import health_platform.utils.paths. "
                "Pass --db-path explicitly or run from repo root."
            )
            sys.exit(1)

    print(f"Connecting to DuckDB: {db_path}")
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    results: dict[str, bool] = {}

    for sql_file in sql_files:
        sql = sql_file.read_text()
        try:
            # Split on semicolons to handle files with multiple statements
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            for stmt in statements:
                con.execute(stmt)
            results[sql_file.name] = True
            print(f"  OK  {sql_file.name}")
        except Exception as e:
            results[sql_file.name] = False
            print(f"  FAIL {sql_file.name}: {e}")

    con.close()

    # Summary
    ok_count = sum(1 for v in results.values() if v)
    fail_count = sum(1 for v in results.values() if not v)
    print()
    print(f"Results: {ok_count} succeeded, {fail_count} failed out of {len(results)}")

    if fail_count > 0:
        print("\nFailed files:")
        for name, success in results.items():
            if not success:
                print(f"  - {name}")

    return results


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Create gold views in local DuckDB")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print SQL without executing",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to DuckDB database file (overrides auto-resolution)",
    )
    args = parser.parse_args()

    results = run_gold_views(db_path=args.db_path, dry_run=args.dry_run)

    if any(not v for v in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
