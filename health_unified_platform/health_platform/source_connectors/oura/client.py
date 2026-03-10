"""
Oura API V2 client.
Wraps each endpoint in a typed method with automatic pagination.
Implements BaseConnector for a consistent connector interface.
"""

from __future__ import annotations

from datetime import date

import requests
from health_platform.source_connectors.base import BaseConnector

BASE_URL = "https://api.ouraring.com"

# Mapping from endpoint name to client method name
ENDPOINT_METHODS: dict[str, str] = {
    "daily_sleep": "fetch_daily_sleep",
    "daily_activity": "fetch_daily_activity",
    "daily_readiness": "fetch_daily_readiness",
    "heartrate": "fetch_heartrate",
    "workout": "fetch_workout",
    "daily_spo2": "fetch_daily_spo2",
    "daily_stress": "fetch_daily_stress",
    "daily_cardiovascular_age": "fetch_daily_cardiovascular_age",
    "daily_resilience": "fetch_daily_resilience",
    "sleep_time": "fetch_sleep_time",
    "enhanced_tag": "fetch_enhanced_tag",
    "vo2_max": "fetch_vo2_max",
    "session": "fetch_session",
    "tag": "fetch_tag",
    "rest_mode_period": "fetch_rest_mode_period",
    "ring_configuration": "fetch_ring_configuration",
    "sleep": "fetch_sleep",
}


class OuraClient(BaseConnector):
    """Oura Ring API V2 connector.

    Implements BaseConnector with Oura-specific authentication and
    paginated data fetching across all available endpoints.
    """

    def __init__(self, access_token: str) -> None:
        self._access_token = access_token
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {access_token}"})

    # ------------------------------------------------------------------
    # BaseConnector interface
    # ------------------------------------------------------------------

    @property
    def source_name(self) -> str:
        return "oura"

    def authenticate(self) -> None:
        """No-op: OuraClient receives a pre-authenticated token at init.

        For the full OAuth flow, see auth.py / get_access_token().
        """

    def fetch(self, endpoint: str, start_date: str, end_date: str) -> list[dict]:
        """Fetch records from the named Oura endpoint.

        Args:
            endpoint: One of the keys in ENDPOINT_METHODS (e.g. 'daily_sleep').
            start_date: ISO 8601 date string (YYYY-MM-DD).
            end_date: ISO 8601 date string (YYYY-MM-DD).

        Returns:
            List of record dicts from the Oura API.
        """
        method_name = ENDPOINT_METHODS.get(endpoint)
        if not method_name:
            raise ValueError(
                f"Unknown endpoint '{endpoint}'. Available: {list(ENDPOINT_METHODS.keys())}"
            )
        method = getattr(self, method_name)
        return method(date.fromisoformat(start_date), date.fromisoformat(end_date))

    def get_endpoints(self) -> list[str]:
        return list(ENDPOINT_METHODS.keys())

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
    # Endpoints — original 7
    # ------------------------------------------------------------------

    def fetch_daily_sleep(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/daily_sleep", self._date_params(start, end)
        )

    def fetch_daily_activity(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/daily_activity", self._date_params(start, end)
        )

    def fetch_daily_readiness(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/daily_readiness", self._date_params(start, end)
        )

    def fetch_heartrate(self, start: date, end: date) -> list[dict]:
        """Heart rate uses full datetime range and must be fetched in chunks.

        Oura rejects ranges larger than ~7 days for this high-frequency endpoint.
        """
        from datetime import timedelta

        records = []
        chunk_days = 7
        chunk_start = start
        while chunk_start <= end:
            chunk_end = min(chunk_start + timedelta(days=chunk_days - 1), end)
            records.extend(
                self._get_collection(
                    "/v2/usercollection/heartrate",
                    self._datetime_params(chunk_start, chunk_end),
                )
            )
            chunk_start = chunk_end + timedelta(days=1)
        return records

    def fetch_workout(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/workout", self._date_params(start, end)
        )

    def fetch_daily_spo2(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/daily_spo2", self._date_params(start, end)
        )

    def fetch_daily_stress(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/daily_stress", self._date_params(start, end)
        )

    def fetch_personal_info(self) -> dict:
        """Personal info is a single object, not a paginated collection."""
        response = self.session.get(f"{BASE_URL}/v2/usercollection/personal_info")
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Endpoints — DS1 expansion (10 new)
    # ------------------------------------------------------------------

    def fetch_daily_cardiovascular_age(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/daily_cardiovascular_age",
            self._date_params(start, end),
        )

    def fetch_daily_resilience(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/daily_resilience",
            self._date_params(start, end),
        )

    def fetch_sleep_time(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/sleep_time",
            self._date_params(start, end),
        )

    def fetch_enhanced_tag(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/enhanced_tag",
            self._date_params(start, end),
        )

    def fetch_vo2_max(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/vo2_max",
            self._date_params(start, end),
        )

    def fetch_session(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/session",
            self._date_params(start, end),
        )

    def fetch_tag(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/tag",
            self._date_params(start, end),
        )

    def fetch_rest_mode_period(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/rest_mode_period",
            self._date_params(start, end),
        )

    def fetch_ring_configuration(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/ring_configuration",
            self._date_params(start, end),
        )

    def fetch_sleep(self, start: date, end: date) -> list[dict]:
        return self._get_collection(
            "/v2/usercollection/sleep",
            self._date_params(start, end),
        )
