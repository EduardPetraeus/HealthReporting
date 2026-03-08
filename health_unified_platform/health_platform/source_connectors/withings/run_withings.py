"""
Entry point for the Withings data pipeline.

First run:  browser opens for OAuth login, 90-day backfill begins.
Subsequent: tokens refreshed if needed, only new data fetched per endpoint.

Usage:
    HEALTH_ENV=dev python run_withings.py
"""

from __future__ import annotations

import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from auth import get_access_token
from client import WithingsClient

from health_platform.utils.logging_config import get_logger
from health_platform.utils.audit_logger import AuditLogger

# Reuse the Oura writer and state modules — same parquet pattern
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "oura"))
from writer import write_records
from state import get_start_date, load_state, save_state, update_state

logger = get_logger("run_withings")

SOURCE_ENV = os.getenv("HEALTH_ENV", "dev")
END_DATE = date.today()

# (endpoint_name, client_method, date_field_in_response)
ENDPOINTS: list[tuple[str, str, str]] = [
    ("weight", "fetch_weight", "datetime"),
    ("blood_pressure", "fetch_blood_pressure", "datetime"),
    ("temperature", "fetch_temperature", "datetime"),
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
            start_date = get_start_date(endpoint_name, state)

            if start_date > END_DATE:
                logger.info(f"{endpoint_name}: already up to date, skipping.")
                continue

            logger.info(f"{endpoint_name}: fetching {start_date} -> {END_DATE}...")
            records = getattr(client, method_name)(start_date, END_DATE)
            logger.info(f"  Fetched {len(records):,} records.")
            write_records(records, endpoint_name, date_field, source_env=SOURCE_ENV)
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
