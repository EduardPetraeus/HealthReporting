"""
Oura API V2 client.
Wraps each endpoint in a typed method with automatic pagination.
"""

from __future__ import annotations

from datetime import date

import requests

BASE_URL = "https://api.ouraring.com"


class OuraClient:
    def __init__(self, access_token: str) -> None:
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {access_token}"})

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_collection(self, path: str, params: dict) -> list[dict]:
        """Fetches all pages from a paginated collection endpoint."""
        records = []
        url = f"{BASE_URL}{path}"
        while url:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            body = response.json()
            records.extend(body.get("data", []))
            next_token = body.get("next_token")
            if next_token:
                url = f"{BASE_URL}{path}"
                params = {"next_token": next_token}
            else:
                url = None
        return records

    def _date_params(self, start: date, end: date) -> dict:
        return {"start_date": start.isoformat(), "end_date": end.isoformat()}

    def _datetime_params(self, start: date, end: date) -> dict:
        """Used for endpoints that require full ISO 8601 datetime range."""
        return {
            "start_datetime": f"{start.isoformat()}T00:00:00Z",
            "end_datetime": f"{end.isoformat()}T23:59:59Z",
        }

    # ------------------------------------------------------------------
    # Endpoints
    # ------------------------------------------------------------------

    def fetch_daily_sleep(self, start: date, end: date) -> list[dict]:
        return self._get_collection("/v2/usercollection/daily_sleep", self._date_params(start, end))

    def fetch_daily_activity(self, start: date, end: date) -> list[dict]:
        return self._get_collection("/v2/usercollection/daily_activity", self._date_params(start, end))

    def fetch_daily_readiness(self, start: date, end: date) -> list[dict]:
        return self._get_collection("/v2/usercollection/daily_readiness", self._date_params(start, end))

    def fetch_heartrate(self, start: date, end: date) -> list[dict]:
        """Heart rate uses full datetime range, not date-only."""
        return self._get_collection("/v2/usercollection/heartrate", self._datetime_params(start, end))

    def fetch_workout(self, start: date, end: date) -> list[dict]:
        return self._get_collection("/v2/usercollection/workout", self._date_params(start, end))

    def fetch_daily_spo2(self, start: date, end: date) -> list[dict]:
        return self._get_collection("/v2/usercollection/daily_spo2", self._date_params(start, end))

    def fetch_daily_stress(self, start: date, end: date) -> list[dict]:
        return self._get_collection("/v2/usercollection/daily_stress", self._date_params(start, end))

    def fetch_personal_info(self) -> dict:
        """Personal info is a single object, not a paginated collection."""
        response = self.session.get(f"{BASE_URL}/v2/usercollection/personal_info")
        response.raise_for_status()
        return response.json()
