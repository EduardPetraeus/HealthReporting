"""
Entry point for the Open-Meteo weather data pipeline.

Fetches daily weather data (last 90 days by default) and writes to parquet
in the data lake with Hive-style partitioning.

Usage:
    HEALTH_ENV=dev python run_weather.py
"""

from __future__ import annotations

import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from client import WeatherClient
from health_platform.utils.logging_config import get_logger
from health_platform.utils.paths import get_data_lake_root

logger = get_logger("run_weather")

SOURCE_ENV = os.getenv("HEALTH_ENV", "dev")
PAST_DAYS = int(os.getenv("WEATHER_PAST_DAYS", "90"))

DATA_LAKE_ROOT = get_data_lake_root() / "weather" / "raw" / "open_meteo"


def _partition_path(partition_date: date) -> Path:
    return (
        DATA_LAKE_ROOT
        / f"year={partition_date.year}"
        / f"month={partition_date.month:02d}"
        / f"day={partition_date.day:02d}"
    )


def _write_partition(df: pd.DataFrame, partition_date: date) -> None:
    path = _partition_path(partition_date)
    path.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path / "data.parquet", compression="snappy")
    logger.info("Written %s rows -> %s/data.parquet", f"{len(df):,}", path)


def write_weather_records(records: list[dict]) -> None:
    """Write weather records partitioned by date."""
    if not records:
        logger.warning("No weather records to write.")
        return

    ingested_at = datetime.now(timezone.utc).isoformat()
    for record in records:
        record["_ingested_at"] = ingested_at
        record["_source_env"] = SOURCE_ENV

    df = pd.DataFrame(records)

    # Parse date field for partitioning
    df["_partition_date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    missing = df["_partition_date"].isna().sum()
    if missing:
        logger.warning("%d records with unparseable date skipped.", missing)

    for partition_date, group in df.dropna(subset=["_partition_date"]).groupby(
        "_partition_date"
    ):
        _write_partition(group.drop(columns=["_partition_date"]), partition_date)


def main() -> None:
    logger.info(
        "Weather pipeline starting [env: %s, past_days: %d]", SOURCE_ENV, PAST_DAYS
    )

    client = WeatherClient()
    records = client.fetch_daily_weather(past_days=PAST_DAYS)
    logger.info("Fetched %d daily weather records.", len(records))

    write_weather_records(records)

    logger.info("Weather pipeline complete.")


if __name__ == "__main__":
    main()
