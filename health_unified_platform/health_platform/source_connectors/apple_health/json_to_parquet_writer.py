"""Convert Health Auto Export (HAE) JSON payloads to partitioned Parquet files.

Produces output identical in schema and partitioning to process_health_data.py,
so the existing ingestion pipeline (ingestion_engine.py → Bronze → Silver) works
without modification.

The HAE app sends JSON with structure:
    {
      "data": {
        "metrics": [
          {
            "name": "step_count",
            "units": "count",
            "data": [
              {"date": "2026-03-07 12:00:00 +0100", "qty": 1234.0, "source": "Apple Watch"},
              ...
            ]
          },
          ...
        ]
      }
    }

Each metric name is resolved to an HKQuantityTypeIdentifier via hae_metric_mapping.yaml,
then get_area() and clean_name() produce the same domain/type used by the XML pipeline.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

from health_platform.source_connectors.apple_health.process_health_data import (
    clean_name,
    get_area,
)
from health_platform.utils.logging_config import get_logger

logger = get_logger("hae_writer")

_MAPPING_PATH = Path(__file__).parent / "hae_metric_mapping.yaml"
_mapping_cache: dict | None = None

# Safety cap: maximum records from a single HAE payload.
# Normal daily payload is ~2-5K records. 50K allows headroom without OOM risk.
MAX_RECORDS = 50_000


def _load_mapping() -> dict[str, dict]:
    """Load and cache the HAE metric → HK type mapping."""
    global _mapping_cache
    if _mapping_cache is None:
        with open(_MAPPING_PATH) as f:
            raw = yaml.safe_load(f)
        _mapping_cache = raw.get("metrics", {})
    return _mapping_cache


def parse_hae_payload(payload: dict) -> list[dict]:
    """Parse a Health Auto Export JSON payload into flat records.

    Each record matches the schema produced by process_health_data.py:
        type, sourceName, unit, startDate, endDate, value,
        data_domain, data_type, duration_seconds, year, _ingested_at

    Unknown metric names (not in hae_metric_mapping.yaml) are skipped with a warning.

    Returns:
        List of record dicts ready for write_hae_records().
    """
    mapping = _load_mapping()
    ingested_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    metrics = payload.get("data", {}).get("metrics", [])
    records: list[dict] = []

    for metric in metrics:
        metric_name = metric.get("name", "")
        mapped = mapping.get(metric_name)
        if mapped is None:
            safe_name = repr(metric_name[:64])
            logger.warning("Unknown HAE metric %s — skipped", safe_name)
            continue

        hk_type = mapped["hk_type"]
        domain = get_area(hk_type)
        data_type = clean_name(hk_type)
        unit = metric.get("units", mapped.get("unit", ""))

        for sample in metric.get("data", []):
            if len(records) >= MAX_RECORDS:
                logger.warning(
                    "HAE record cap reached (%d); truncating payload", MAX_RECORDS
                )
                return records

            start_date = sample.get("date", "")
            qty = sample.get("qty")
            source = sample.get("source", "")

            records.append(
                {
                    "type": hk_type,
                    "sourceName": source,
                    "unit": unit,
                    "startDate": start_date,
                    # HAE samples are point-in-time; endDate = startDate → duration_seconds = 0.0
                    "endDate": start_date,
                    "value": qty,
                    "data_domain": domain,
                    "data_type": data_type,
                    "_ingested_at": ingested_at,
                }
            )

    return records


def write_hae_records(records: list[dict], output_root: Path) -> dict:
    """Write HAE records to partitioned Parquet files.

    Uses the same partitioning as process_health_data.py:
        {output_root}/{domain}/{data_type}/year={YYYY}/hae_{ingested_at}.parquet

    Args:
        records: Flat record dicts from parse_hae_payload().
        output_root: Data lake root for apple_health_data
                     (e.g. /Users/Shared/data_lake/apple_health_data).

    Returns:
        Dict with keys: records_written (int), files_created (int).
    """
    if not records:
        return {"records_written": 0, "files_created": 0}

    df = pd.DataFrame(records)

    # Match process_health_data.py type coercions
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    if "startDate" in df.columns:
        df["startDate"] = pd.to_datetime(df["startDate"], utc=True, errors="coerce")
        bad_dates = df["startDate"].isna().sum()
        if bad_dates:
            logger.warning("Dropped %d records with unparseable dates", bad_dates)
            df = df.dropna(subset=["startDate"])
        if df.empty:
            return {"records_written": 0, "files_created": 0}
        df["year"] = df["startDate"].dt.year
    else:
        df["year"] = 0

    if "startDate" in df.columns and "endDate" in df.columns:
        df["endDate"] = pd.to_datetime(df["endDate"], utc=True, errors="coerce")
        df["duration_seconds"] = (df["endDate"] - df["startDate"]).dt.total_seconds()

    ingested_at = records[0].get("_ingested_at", "unknown")
    filename = f"hae_{ingested_at}.parquet"
    files_written = 0

    for (domain, d_type, year), group in df.groupby(
        ["data_domain", "data_type", "year"]
    ):
        target_path = output_root / domain / d_type / f"year={year}"
        target_path.mkdir(parents=True, exist_ok=True)
        group.to_parquet(target_path / filename, index=False, compression="snappy")
        files_written += 1

    logger.info(
        "HAE write complete: %d records → %d parquet files", len(df), files_written
    )
    return {"records_written": len(df), "files_created": files_written}
