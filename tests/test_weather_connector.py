"""
Tests for the Open-Meteo weather connector.
Uses mock responses — no real API calls.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Make source_connectors importable
sys.path.insert(
    0,
    str(
        Path(__file__).resolve().parents[1]
        / "health_unified_platform"
        / "health_platform"
        / "source_connectors"
        / "weather"
    ),
)

from client import WeatherClient

# Synthetic Open-Meteo API response matching the real format
MOCK_API_RESPONSE = {
    "latitude": 55.68,
    "longitude": 12.56,
    "generationtime_ms": 0.5,
    "utc_offset_seconds": 3600,
    "timezone": "Europe/Copenhagen",
    "daily_units": {
        "time": "iso8601",
        "temperature_2m_max": "\u00b0C",
        "temperature_2m_min": "\u00b0C",
        "precipitation_sum": "mm",
        "wind_speed_10m_max": "km/h",
        "uv_index_max": "",
    },
    "daily": {
        "time": ["2026-03-01", "2026-03-02", "2026-03-03"],
        "temperature_2m_max": [5.2, 7.1, 3.8],
        "temperature_2m_min": [-1.0, 0.5, -2.3],
        "precipitation_sum": [0.0, 2.4, 0.1],
        "wind_speed_10m_max": [22.5, 15.3, 31.0],
        "uv_index_max": [1.5, 1.2, 0.8],
    },
}


class TestWeatherClientParsing:
    """Test that the client correctly parses Open-Meteo API responses."""

    def test_parse_daily_response_returns_correct_count(self) -> None:
        records = WeatherClient._parse_daily_response(MOCK_API_RESPONSE)
        assert len(records) == 3

    def test_parse_daily_response_keys(self) -> None:
        records = WeatherClient._parse_daily_response(MOCK_API_RESPONSE)
        expected_keys = {
            "date",
            "temp_max_c",
            "temp_min_c",
            "precipitation_mm",
            "wind_speed_max_kmh",
            "uv_index_max",
        }
        assert set(records[0].keys()) == expected_keys

    def test_parse_daily_response_values(self) -> None:
        records = WeatherClient._parse_daily_response(MOCK_API_RESPONSE)
        first = records[0]
        assert first["date"] == "2026-03-01"
        assert first["temp_max_c"] == 5.2
        assert first["temp_min_c"] == -1.0
        assert first["precipitation_mm"] == 0.0
        assert first["wind_speed_max_kmh"] == 22.5
        assert first["uv_index_max"] == 1.5

    def test_parse_daily_response_last_record(self) -> None:
        records = WeatherClient._parse_daily_response(MOCK_API_RESPONSE)
        last = records[-1]
        assert last["date"] == "2026-03-03"
        assert last["temp_max_c"] == 3.8
        assert last["temp_min_c"] == -2.3

    def test_parse_empty_response(self) -> None:
        empty = {"daily": {"time": []}}
        records = WeatherClient._parse_daily_response(empty)
        assert records == []

    def test_parse_missing_daily_key(self) -> None:
        records = WeatherClient._parse_daily_response({})
        assert records == []


class TestWeatherClientFetch:
    """Test the fetch method with mocked HTTP calls."""

    @patch("client.requests.Session")
    def test_fetch_daily_weather_calls_api(self, mock_session_cls: MagicMock) -> None:
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = MOCK_API_RESPONSE
        mock_response.raise_for_status.return_value = None
        mock_session.get.return_value = mock_response
        mock_session_cls.return_value = mock_session

        client = WeatherClient(latitude=55.68, longitude=12.56)
        client.session = mock_session
        records = client.fetch_daily_weather(past_days=7)

        mock_session.get.assert_called_once()
        assert len(records) == 3

    @patch("client.requests.Session")
    def test_fetch_passes_correct_params(self, mock_session_cls: MagicMock) -> None:
        mock_session = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = MOCK_API_RESPONSE
        mock_response.raise_for_status.return_value = None
        mock_session.get.return_value = mock_response
        mock_session_cls.return_value = mock_session

        client = WeatherClient(latitude=55.68, longitude=12.56, timezone="Europe/Copenhagen")
        client.session = mock_session
        client.fetch_daily_weather(past_days=30)

        call_args = mock_session.get.call_args
        params = call_args.kwargs.get("params") or call_args[1].get("params")
        assert params["latitude"] == 55.68
        assert params["longitude"] == 12.56
        assert params["past_days"] == 30
        assert "temperature_2m_max" in params["daily"]


class TestMergeSqlValidity:
    """Test that the merge SQL file exists and has valid structure."""

    MERGE_SQL_PATH = (
        Path(__file__).resolve().parents[1]
        / "health_unified_platform"
        / "health_platform"
        / "transformation_logic"
        / "dbt"
        / "merge"
        / "silver"
        / "merge_weather_daily.sql"
    )

    def test_merge_sql_file_exists(self) -> None:
        assert self.MERGE_SQL_PATH.exists(), f"Merge SQL not found at {self.MERGE_SQL_PATH}"

    def test_merge_sql_contains_staging_table(self) -> None:
        sql = self.MERGE_SQL_PATH.read_text()
        assert "CREATE OR REPLACE TABLE silver.daily_weather__staging" in sql

    def test_merge_sql_contains_merge_statement(self) -> None:
        sql = self.MERGE_SQL_PATH.read_text()
        assert "MERGE INTO silver.daily_weather" in sql

    def test_merge_sql_contains_business_key(self) -> None:
        sql = self.MERGE_SQL_PATH.read_text()
        assert "business_key_hash" in sql

    def test_merge_sql_contains_row_hash(self) -> None:
        sql = self.MERGE_SQL_PATH.read_text()
        assert "row_hash" in sql

    def test_merge_sql_drops_staging(self) -> None:
        sql = self.MERGE_SQL_PATH.read_text()
        assert "DROP TABLE IF EXISTS silver.daily_weather__staging" in sql

    def test_merge_sql_contains_all_columns(self) -> None:
        sql = self.MERGE_SQL_PATH.read_text()
        for col in ["temp_max_c", "temp_min_c", "precipitation_mm", "wind_speed_max_kmh", "uv_index_max"]:
            assert col in sql, f"Column {col} missing from merge SQL"
