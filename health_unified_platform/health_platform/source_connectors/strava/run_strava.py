"""
Entry point for the Strava data pipeline.

First run:  browser opens for OAuth login, 90-day backfill begins.
Subsequent: tokens refreshed if needed, only new data fetched per endpoint.

Usage:
    HEALTH_ENV=dev python run_strava.py
"""

from __future__ import annotations

import os
from datetime import date

from auth import get_access_token
from client import StravaClient
from health_platform.source_connectors.oura.state import (
    get_start_date,
    load_state,
    save_state,
    update_state,
)
from health_platform.source_connectors.oura.writer import write_records
from health_platform.utils.audit_logger import AuditLogger
from health_platform.utils.logging_config import get_logger

logger = get_logger("run_strava")

SOURCE_ENV = os.getenv("HEALTH_ENV", "dev")
END_DATE = date.today()

# (endpoint_name, client_method, date_field_in_response)
ENDPOINTS: list[tuple[str, str, str]] = [
    ("activities", "fetch_activities", "start_date"),
]


def main() -> None:
    logger.info(f"Strava pipeline starting [env: {SOURCE_ENV}]")

    with AuditLogger("run_strava", "extract", "strava") as audit:
        access_token = get_access_token()
        client = StravaClient(access_token)
        state = load_state()

        # Fetch dated endpoints
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
                f"strava.{endpoint_name}",
                "WRITE_PARQUET",
                rows_after=len(records),
                status="success",
            )

        # athlete_stats has no date range — always refresh
        logger.info("athlete_stats: fetching...")
        stats = client.fetch_athlete_stats()
        write_records([stats], "athlete_stats", date_field="", source_env=SOURCE_ENV)
        audit.log_table(
            "strava.athlete_stats", "WRITE_PARQUET", rows_after=1, status="success"
        )

        save_state(state)

    logger.info("Strava pipeline complete")


if __name__ == "__main__":
    main()
