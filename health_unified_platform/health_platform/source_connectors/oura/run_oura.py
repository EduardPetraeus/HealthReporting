"""
Entry point for the Oura Ring data pipeline.

First run:  browser opens for OAuth login, 90-day backfill begins.
Subsequent: tokens refreshed if needed, only new data fetched per endpoint.

Usage:
    HEALTH_ENV=dev python run_oura.py
"""

from __future__ import annotations

import os
from datetime import date

from auth import get_access_token
from client import OuraClient
from health_platform.utils.audit_logger import AuditLogger
from health_platform.utils.logging_config import get_logger
from state import clean_state, get_start_date, load_state, update_state
from writer import write_records

logger = get_logger("run_oura")

SOURCE_ENV = os.getenv("HEALTH_ENV", "dev")
END_DATE = date.today()

# (endpoint_name, client_method, date_field_in_response)
# date_field is used to determine the partition date for each record
ENDPOINTS: list[tuple[str, str, str]] = [
    ("daily_sleep", "fetch_daily_sleep", "day"),
    ("daily_activity", "fetch_daily_activity", "day"),
    ("daily_readiness", "fetch_daily_readiness", "day"),
    ("heartrate", "fetch_heartrate", "timestamp"),
    ("workout", "fetch_workout", "day"),
    ("daily_spo2", "fetch_daily_spo2", "day"),
    ("daily_stress", "fetch_daily_stress", "day"),
    ("daily_cardiovascular_age", "fetch_daily_cardiovascular_age", "day"),
    ("daily_resilience", "fetch_daily_resilience", "day"),
    ("sleep_time", "fetch_sleep_time", "day"),
    ("enhanced_tag", "fetch_enhanced_tag", "timestamp"),
    ("vo2_max", "fetch_vo2_max", "day"),
    ("session", "fetch_session", "day"),
    ("tag", "fetch_tag", "timestamp"),
    ("rest_mode_period", "fetch_rest_mode_period", "start_day"),
    ("ring_configuration", "fetch_ring_configuration", "set_date"),
    ("sleep", "fetch_sleep", "day"),
]


def _normalize_records(endpoint_name: str, records: list[dict]) -> list[dict]:
    """
    Applies endpoint-specific normalization before writing.
    Ensures consistent types across parquet files to prevent schema conflicts on read.
    """
    if endpoint_name == "daily_spo2":
        for r in records:
            spo2 = r.get("spo2_percentage")
            r["spo2_percentage"] = {
                "average": spo2.get("average") if isinstance(spo2, dict) else None
            }
    return records


def main() -> None:
    logger.info(f"Oura pipeline starting [env: {SOURCE_ENV}]")

    with AuditLogger("run_oura", "extract", "oura") as audit:
        access_token = get_access_token()
        client = OuraClient(access_token)
        state = load_state()

        # Remove ghost entries from state that no longer match active endpoints
        valid_names = [ep[0] for ep in ENDPOINTS] + ["personal_info"]
        state = clean_state(valid_names, state)

        # Fetch all dated endpoints
        for endpoint_name, method_name, date_field in ENDPOINTS:
            start_date = get_start_date(endpoint_name, state)

            if start_date > END_DATE:
                logger.info(f"{endpoint_name}: already up to date, skipping.")
                continue

            logger.info(f"{endpoint_name}: fetching {start_date} -> {END_DATE}...")
            records = getattr(client, method_name)(start_date, END_DATE)
            logger.info(f"  Fetched {len(records):,} records.")
            records = _normalize_records(endpoint_name, records)
            write_records(records, endpoint_name, date_field, source_env=SOURCE_ENV)
            update_state(endpoint_name, END_DATE, state)

            audit.log_table(
                f"oura.{endpoint_name}",
                "WRITE_PARQUET",
                rows_after=len(records),
                status="success",
            )

        # personal_info has no date range — always refresh
        logger.info("personal_info: fetching...")
        personal_info = client.fetch_personal_info()
        write_records(
            [personal_info], "personal_info", date_field="", source_env=SOURCE_ENV
        )
        audit.log_table(
            "oura.personal_info", "WRITE_PARQUET", rows_after=1, status="success"
        )

    logger.info("Oura pipeline complete")


if __name__ == "__main__":
    main()
