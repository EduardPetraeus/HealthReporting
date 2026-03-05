"""
Open-Meteo weather API client.
Fetches daily weather data for a configurable location (default: Copenhagen).
No authentication required.
"""

from __future__ import annotations

import os
from datetime import date

import requests

BASE_URL = "https://api.open-meteo.com/v1/forecast"

# Daily weather variables to fetch
DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "wind_speed_10m_max",
    "uv_index_max",
]

# Default location: Copenhagen, Denmark
DEFAULT_LATITUDE = 55.6761
DEFAULT_LONGITUDE = 12.5683
DEFAULT_TIMEZONE = "Europe/Copenhagen"


class WeatherClient:
    """Fetches daily weather data from the Open-Meteo API."""

    def __init__(
        self,
        latitude: float | None = None,
        longitude: float | None = None,
        timezone: str | None = None,
    ) -> None:
        self.latitude = latitude or float(os.getenv("WEATHER_LATITUDE", str(DEFAULT_LATITUDE)))
        self.longitude = longitude or float(os.getenv("WEATHER_LONGITUDE", str(DEFAULT_LONGITUDE)))
        self.timezone = timezone or os.getenv("WEATHER_TIMEZONE", DEFAULT_TIMEZONE)
        self.session = requests.Session()

    def fetch_daily_weather(self, past_days: int = 90) -> list[dict]:
        """
        Fetch daily weather data for the last N days.

        Returns a list of dicts, one per day, with keys:
            date, temp_max_c, temp_min_c, precipitation_mm,
            wind_speed_max_kmh, uv_index_max
        """
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "daily": ",".join(DAILY_VARIABLES),
            "timezone": self.timezone,
            "past_days": past_days,
        }

        response = self.session.get(BASE_URL, params=params)
        response.raise_for_status()
        body = response.json()

        return self._parse_daily_response(body)

    @staticmethod
    def _parse_daily_response(body: dict) -> list[dict]:
        """Parse the Open-Meteo daily response into a flat list of records."""
        daily = body.get("daily", {})
        dates = daily.get("time", [])
        temp_max = daily.get("temperature_2m_max", [])
        temp_min = daily.get("temperature_2m_min", [])
        precipitation = daily.get("precipitation_sum", [])
        wind_speed = daily.get("wind_speed_10m_max", [])
        uv_index = daily.get("uv_index_max", [])

        records = []
        for i, day_str in enumerate(dates):
            records.append(
                {
                    "date": day_str,
                    "temp_max_c": temp_max[i] if i < len(temp_max) else None,
                    "temp_min_c": temp_min[i] if i < len(temp_min) else None,
                    "precipitation_mm": precipitation[i] if i < len(precipitation) else None,
                    "wind_speed_max_kmh": wind_speed[i] if i < len(wind_speed) else None,
                    "uv_index_max": uv_index[i] if i < len(uv_index) else None,
                }
            )
        return records
