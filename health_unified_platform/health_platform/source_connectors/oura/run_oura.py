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
from state import get_start_date, load_state, save_state, update_state
from writer import write_records

SOURCE_ENV = os.getenv("HEALTH_ENV", "dev")
END_DATE = date.today()

# (endpoint_name, client_method, date_field_in_response)
# date_field is used to determine the partition date for each record
ENDPOINTS: list[tuple[str, str, str]] = [
    ("daily_sleep",     "fetch_daily_sleep",     "day"),
    ("daily_activity",  "fetch_daily_activity",  "day"),
    ("daily_readiness", "fetch_daily_readiness", "day"),
    ("heartrate",       "fetch_heartrate",       "timestamp"),
    ("workout",         "fetch_workout",         "day"),
    ("daily_spo2",      "fetch_daily_spo2",      "day"),
    ("daily_stress",    "fetch_daily_stress",    "day"),
]


def main() -> None:
    print(f"--- Oura pipeline starting [env: {SOURCE_ENV}] ---")

    access_token = get_access_token()
    client = OuraClient(access_token)
    state = load_state()

    # Fetch all dated endpoints
    for endpoint_name, method_name, date_field in ENDPOINTS:
        start_date = get_start_date(endpoint_name, state)

        if start_date > END_DATE:
            print(f"{endpoint_name}: already up to date, skipping.")
            continue

        print(f"{endpoint_name}: fetching {start_date} -> {END_DATE}...")
        records = getattr(client, method_name)(start_date, END_DATE)
        print(f"  Fetched {len(records):,} records.")
        write_records(records, endpoint_name, date_field, source_env=SOURCE_ENV)
        update_state(endpoint_name, END_DATE, state)

    # personal_info has no date range — always refresh
    print("personal_info: fetching...")
    personal_info = client.fetch_personal_info()
    write_records([personal_info], "personal_info", date_field="", source_env=SOURCE_ENV)

    save_state(state)
    print("--- Oura pipeline complete ---")


if __name__ == "__main__":
    main()
