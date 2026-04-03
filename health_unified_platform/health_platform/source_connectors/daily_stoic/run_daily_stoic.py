"""
Entry point for the Daily Stoic data pipeline.

Reads tables from the local daily-stoic DuckDB database and writes each
as a flat parquet file in the data lake.

Usage:
    HEALTH_ENV=dev python run_daily_stoic.py
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from health_platform.utils.logging_config import get_logger
from health_platform.utils.paths import get_data_lake_root

logger = get_logger("run_daily_stoic")

SOURCE_ENV = os.getenv("HEALTH_ENV", "dev")

TABLES = [
    "focus",
    "habit_definitions",
    "habits",
    "quotes",
    "reflections",
]


def _source_path() -> Path:
    """Return the path to the daily-stoic DuckDB file."""
    return Path.home() / "Dagbog" / "daily-stoic.duckdb"


def _output_path(table_name: str) -> Path:
    """Return the output parquet path for a given table."""
    return get_data_lake_root() / "daily_stoic" / table_name / "data.parquet"


def _extract_table(con: duckdb.DuckDBPyConnection, table_name: str) -> pd.DataFrame:
    """Read a full table from the source database into a DataFrame."""
    return con.execute(f"SELECT * FROM {table_name}").fetchdf()


def _add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Add ingestion metadata columns."""
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source_system"] = "daily_stoic"
    return df


def _write_parquet(df: pd.DataFrame, table_name: str) -> None:
    """Write a DataFrame as a flat parquet file."""
    path = _output_path(table_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression="snappy")
    logger.info("Written %s rows -> %s", f"{len(df):,}", path)


def main() -> None:
    """Extract all tables from daily-stoic and write to parquet."""
    logger.info("Daily Stoic pipeline starting [env: %s]", SOURCE_ENV)

    source = _source_path()
    logger.info("Source database: %s", source)
    con = duckdb.connect(str(source), read_only=True)
    try:
        for table_name in TABLES:
            df = _extract_table(con, table_name)
            df = _add_metadata(df)
            _write_parquet(df, table_name)
    finally:
        con.close()
    logger.info("Daily Stoic pipeline complete.")


if __name__ == "__main__":
    main()
