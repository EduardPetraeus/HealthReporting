"""
Entry point for the Withings data pipeline.

Full load: fetches all data from 2020-01-01 on first run.
Incremental: 30-day overlap window for dedup safety.

Usage:
    HEALTH_ENV=dev python run_withings.py
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path

from auth import get_access_token
from client import WithingsClient
from health_platform.source_connectors.oura.writer import write_records
from health_platform.source_connectors.withings.state import (
    load_state,
    save_state,
    update_state,
)
from health_platform.utils.audit_logger import AuditLogger
from health_platform.utils.logging_config import get_logger

logger = get_logger("run_withings")

SOURCE_ENV = os.getenv("HEALTH_ENV", "dev")
END_DATE = date.today()

FULL_LOAD_START = date(2020, 1, 1)
DELTA_DAYS = 30

# Resolve Withings-specific data lake root
_CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "health_environment"
    / "config"
    / "environment_config.yaml"
)


def _resolve_withings_root() -> Path:
    """Resolve data lake root for Withings (withings/raw instead of oura/raw)."""
    try:
        import yaml

        with open(_CONFIG_PATH) as f:
            cfg = yaml.safe_load(f)
        return Path(cfg["paths"]["data_lake_root"]) / "withings" / "raw"
    except Exception:
        return Path.home() / "health_data_lake" / "withings" / "raw"


WITHINGS_DATA_ROOT = _resolve_withings_root()

# (endpoint_name, client_method, date_field_in_response)
ENDPOINTS: list[tuple[str, str, str]] = [
    ("weight", "fetch_weight", "datetime"),
    ("blood_pressure", "fetch_blood_pressure", "datetime"),
    ("temperature", "fetch_temperature", "datetime"),
    ("activity", "fetch_activity", "date"),
    ("intraday_activity", "fetch_intraday_activity", "datetime"),
    ("workouts", "fetch_workouts", "date"),
    ("sleep_summary", "fetch_sleep_summary", "date"),
    ("sleep_raw", "fetch_sleep_raw", "startdate"),
    ("heart_list", "fetch_heart_list", "timestamp"),
]


def main() -> None:
    logger.info(f"Withings pipeline starting [env: {SOURCE_ENV}]")

    with AuditLogger("run_withings", "extract", "withings") as audit:
        access_token = get_access_token()
        client = WithingsClient(access_token)
        state = load_state()

        for endpoint_name, method_name, date_field in ENDPOINTS:
            if endpoint_name in state:
                start_date = max(
                    date.fromisoformat(state[endpoint_name])
                    - timedelta(days=DELTA_DAYS),
                    FULL_LOAD_START,
                )
            else:
                start_date = FULL_LOAD_START

            if start_date > END_DATE:
                logger.info(f"{endpoint_name}: already up to date, skipping.")
                continue

            logger.info(f"{endpoint_name}: fetching {start_date} -> {END_DATE}...")
            try:
                records = getattr(client, method_name)(start_date, END_DATE)
            except RuntimeError as exc:
                logger.warning(f"{endpoint_name}: API error, skipping — {exc}")
                audit.log_table(
                    f"withings.{endpoint_name}",
                    "WRITE_PARQUET",
                    rows_after=0,
                    status="skipped",
                )
                continue
            logger.info(f"  Fetched {len(records):,} records.")
            write_records(
                records,
                endpoint_name,
                date_field,
                source_env=SOURCE_ENV,
                data_lake_root=WITHINGS_DATA_ROOT,
            )
            update_state(endpoint_name, END_DATE, state)

            audit.log_table(
                f"withings.{endpoint_name}",
                "WRITE_PARQUET",
                rows_after=len(records),
                status="success",
            )

        save_state(state)

    logger.info("Withings pipeline complete")


if __name__ == "__main__":
    main()
