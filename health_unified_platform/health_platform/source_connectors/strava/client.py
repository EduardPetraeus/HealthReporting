"""
Strava API V3 client.
Wraps each endpoint in a typed method with automatic pagination.

API reference: https://developers.strava.com/docs/reference/
"""

from __future__ import annotations

from datetime import date, datetime

import requests

BASE_URL = "https://www.strava.com/api/v3"
PAGE_SIZE = 100  # Strava max per_page


class StravaClient:
    def __init__(self, access_token: str) -> None:
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {access_token}"})

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_paginated(self, path: str, params: dict | None = None) -> list[dict]:
        """Fetches all pages from a paginated list endpoint."""
        params = params or {}
        params["per_page"] = PAGE_SIZE
        records = []
        page = 1
        while True:
            params["page"] = page
            response = self.session.get(f"{BASE_URL}{path}", params=params)
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            records.extend(data)
            if len(data) < PAGE_SIZE:
                break
            page += 1
        return records

    def _date_to_epoch(self, d: date) -> int:
        """Convert a date to Unix epoch timestamp."""
        return int(datetime.combine(d, datetime.min.time()).timestamp())

    # ------------------------------------------------------------------
    # Endpoints
    # ------------------------------------------------------------------

    def fetch_activities(self, start: date, end: date) -> list[dict]:
        """Fetch athlete activities within a date range.

        Returns summary activity records with distance, duration,
        heart rate, power, and elevation data.
        """
        params = {
            "after": self._date_to_epoch(start),
            "before": self._date_to_epoch(end) + 86400,  # include end date
        }
        activities = self._get_paginated("/athlete/activities", params)

        # Flatten to consistent record format
        records = []
        for act in activities:
            records.append({
                "activity_id": act.get("id"),
                "name": act.get("name"),
                "activity_type": act.get("type"),
                "sport_type": act.get("sport_type"),
                "start_date": act.get("start_date"),
                "start_date_local": act.get("start_date_local"),
                "timezone": act.get("timezone"),
                "distance_m": act.get("distance"),
                "moving_time_s": act.get("moving_time"),
                "elapsed_time_s": act.get("elapsed_time"),
                "total_elevation_gain_m": act.get("total_elevation_gain"),
                "average_speed_mps": act.get("average_speed"),
                "max_speed_mps": act.get("max_speed"),
                "average_heartrate": act.get("average_heartrate"),
                "max_heartrate": act.get("max_heartrate"),
                "average_watts": act.get("average_watts"),
                "max_watts": act.get("max_watts"),
                "weighted_average_watts": act.get("weighted_average_watts"),
                "kilojoules": act.get("kilojoules"),
                "suffer_score": act.get("suffer_score"),
                "average_cadence": act.get("average_cadence"),
                "average_temp": act.get("average_temp"),
                "calories": act.get("calories"),
                "has_heartrate": act.get("has_heartrate"),
                "has_power": act.get("device_watts", False),
            })
        return records

    def fetch_activity_detail(self, activity_id: int) -> dict:
        """Fetch detailed data for a single activity."""
        response = self.session.get(f"{BASE_URL}/activities/{activity_id}")
        response.raise_for_status()
        return response.json()

    def fetch_athlete_stats(self) -> dict:
        """Fetch aggregated athlete statistics (totals, recent, YTD).

        Requires the authenticated athlete's ID, which we get from /athlete.
        """
        athlete_response = self.session.get(f"{BASE_URL}/athlete")
        athlete_response.raise_for_status()
        athlete_id = athlete_response.json().get("id")
        response = self.session.get(f"{BASE_URL}/athletes/{athlete_id}/stats")
        response.raise_for_status()
        return response.json()

    def get_endpoints(self) -> list[str]:
        """Return available endpoint names for this source."""
        return ["activities", "athlete_stats"]
