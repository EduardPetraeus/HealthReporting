"""
Parquet writer with Hive-style partitioning for health data.
Output structure: {data_lake_root}/{endpoint}/year=YYYY/month=MM/day=DD/data.parquet
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from health_platform.utils.logging_config import get_logger
from health_platform.utils.paths import get_data_lake_root

logger = get_logger("oura.writer")

DATA_LAKE_ROOT = get_data_lake_root() / "oura" / "raw"


def _partition_path(
    endpoint: str, partition_date: date, root: Path | None = None
) -> Path:
    base = root if root is not None else DATA_LAKE_ROOT
    return (
        base
        / endpoint
        / f"year={partition_date.year}"
        / f"month={partition_date.month:02d}"
        / f"day={partition_date.day:02d}"
    )


def _write_partition(
    df: pd.DataFrame,
    endpoint: str,
    partition_date: date,
    root: Path | None = None,
) -> None:
    path = _partition_path(endpoint, partition_date, root=root)
    path.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path / "data.parquet", compression="snappy")
    logger.info("Written %s rows -> %s/data.parquet", f"{len(df):,}", path)


def write_records(
    records: list[dict],
    endpoint: str,
    date_field: str,
    source_env: str = "dev",
    data_lake_root: Path | None = None,
) -> None:
    """
    Adds metadata columns and writes records partitioned by date_field.
    Each distinct date gets its own parquet file (overwrites on re-run).

    Pass data_lake_root to override the default Oura root (e.g. for Withings).
    """
    if not records:
        logger.warning("No records for %s.", endpoint)
        return

    root = data_lake_root

    ingested_at = datetime.now(timezone.utc).isoformat()
    for record in records:
        record["_ingested_at"] = ingested_at
        record["_source_env"] = source_env

    df = pd.DataFrame(records)

    if date_field not in df.columns:
        # Endpoint has no date field (e.g. personal_info) — write to today
        _write_partition(df, endpoint, date.today(), root=root)
        return

    # Parse date field — handles YYYY-MM-DD strings, ISO 8601 datetimes,
    # and Unix epoch seconds (e.g. Withings heart_list timestamps)
    raw = df[date_field]
    if pd.api.types.is_numeric_dtype(raw):
        df["_partition_date"] = pd.to_datetime(
            raw, unit="s", utc=True, errors="coerce"
        ).dt.date
    else:
        df["_partition_date"] = pd.to_datetime(raw, utc=True, errors="coerce").dt.date

    missing = df["_partition_date"].isna().sum()
    if missing:
        logger.warning("%d records with unparseable %s skipped.", missing, date_field)

    for partition_date, group in df.dropna(subset=["_partition_date"]).groupby(
        "_partition_date"
    ):
        _write_partition(
            group.drop(columns=["_partition_date"]),
            endpoint,
            partition_date,
            root=root,
        )
