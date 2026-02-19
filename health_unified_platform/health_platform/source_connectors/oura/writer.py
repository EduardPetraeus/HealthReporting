"""
Parquet writer with Hive-style partitioning for Oura data.
Output structure: {data_lake_root}/{endpoint}/year=YYYY/month=MM/day=DD/data.parquet
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATA_LAKE_ROOT = Path(os.getenv("OURA_DATA_LAKE_ROOT", "/Users/Shared/data_lake/oura/raw"))


def _partition_path(endpoint: str, partition_date: date) -> Path:
    return (
        DATA_LAKE_ROOT
        / endpoint
        / f"year={partition_date.year}"
        / f"month={partition_date.month:02d}"
        / f"day={partition_date.day:02d}"
    )


def _write_partition(df: pd.DataFrame, endpoint: str, partition_date: date) -> None:
    path = _partition_path(endpoint, partition_date)
    path.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path / "data.parquet", compression="snappy")
    print(f"  Written {len(df):,} rows -> {path}/data.parquet")


def write_records(
    records: list[dict],
    endpoint: str,
    date_field: str,
    source_env: str = "dev",
) -> None:
    """
    Adds metadata columns and writes records partitioned by date_field.
    Each distinct date gets its own parquet file (overwrites on re-run).
    """
    if not records:
        print(f"  No records for {endpoint}.")
        return

    ingested_at = datetime.now(timezone.utc).isoformat()
    for record in records:
        record["_ingested_at"] = ingested_at
        record["_source_env"] = source_env

    df = pd.DataFrame(records)

    if date_field not in df.columns:
        # Endpoint has no date field (e.g. personal_info) — write to today
        _write_partition(df, endpoint, date.today())
        return

    # Parse date field — handles both YYYY-MM-DD strings and ISO 8601 datetimes
    df["_partition_date"] = (
        pd.to_datetime(df[date_field], utc=True, errors="coerce").dt.date
    )

    missing = df["_partition_date"].isna().sum()
    if missing:
        print(f"  Warning: {missing} records with unparseable {date_field} skipped.")

    for partition_date, group in df.dropna(subset=["_partition_date"]).groupby("_partition_date"):
        _write_partition(group.drop(columns=["_partition_date"]), endpoint, partition_date)
