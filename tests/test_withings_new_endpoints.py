"""Tests for new Withings API endpoints added in DS1 expansion."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))

from health_platform.source_connectors.withings.client import WithingsClient


class TestSleepSummary:

    def test_returns_flattened_records(self):
        client = WithingsClient(access_token="test-token")
        mock_body = {
            "series": [
                {
                    "startdate": 1704067200,
                    "enddate": 1704096000,
                    "date": "2024-01-01",
                    "model": 32,
                    "model_id": 63,
                    "data": {
                        "breathing_disturbances_intensity": 10,
                        "deepsleepduration": 3600,
                        "lightsleepduration": 7200,
                        "remsleepduration": 5400,
                        "sleep_score": 75,
                    },
                }
            ]
        }
        with patch.object(client, "_post", return_value=mock_body):
            records = client.fetch_sleep_summary(date(2024, 1, 1), date(2024, 1, 2))
        assert len(records) == 1
        assert records[0]["date"] == "2024-01-01"
        assert records[0]["sleep_score"] == 75
        assert records[0]["deepsleepduration"] == 3600

    def test_empty_response(self):
        client = WithingsClient(access_token="test-token")
        with patch.object(client, "_post", return_value={"series": []}):
            records = client.fetch_sleep_summary(date(2024, 1, 1), date(2024, 1, 2))
        assert records == []


class TestSleepRaw:

    def test_returns_sensor_arrays(self):
        client = WithingsClient(access_token="test-token")
        mock_body = {
            "series": [
                {
                    "startdate": 1704067200,
                    "enddate": 1704096000,
                    "model": 32,
                    "model_id": 63,
                    "hr": {"1704067200": 62, "1704067500": 58},
                    "rr": {"1704067200": 16, "1704067500": 14},
                    "snoring": {"1704067200": 0},
                    "sdnn_1": {"1704067200": 45},
                    "rmssd": {"1704067200": 38},
                }
            ]
        }
        with patch.object(client, "_post", return_value=mock_body):
            records = client.fetch_sleep_raw(date(2024, 1, 1), date(2024, 1, 2))
        assert len(records) == 1
        assert records[0]["hr"] is not None
        assert records[0]["rr"] is not None


class TestHeartList:

    def test_returns_ecg_metadata(self):
        client = WithingsClient(access_token="test-token")
        mock_body = {
            "series": [
                {
                    "signalid": 12345,
                    "ecg": {"afib": 0},
                    "bloodpressure": {},
                    "heart_rate": 72,
                    "model": 44,
                    "deviceid": "abc123",
                    "timestamp": 1704067200,
                }
            ]
        }
        with patch.object(client, "_post", return_value=mock_body):
            records = client.fetch_heart_list(date(2024, 1, 1), date(2024, 1, 2))
        assert len(records) == 1
        assert records[0]["signalid"] == 12345
        assert records[0]["heart_rate"] == 72


class TestHeartSignal:

    def test_returns_signal_body(self):
        client = WithingsClient(access_token="test-token")
        mock_body = {
            "signal": [100, 102, 98, 105],
            "sampling_frequency": 300,
            "wearposition": 1,
        }
        with patch.object(client, "_post", return_value=mock_body):
            result = client.fetch_heart_signal(12345)
        assert result["sampling_frequency"] == 300
        assert len(result["signal"]) == 4


class TestGetEndpoints:

    def test_endpoint_count(self):
        client = WithingsClient(access_token="test-token")
        endpoints = client.get_endpoints()
        assert len(endpoints) == 6

    def test_new_endpoints_included(self):
        client = WithingsClient(access_token="test-token")
        endpoints = client.get_endpoints()
        assert "sleep_summary" in endpoints
        assert "sleep_raw" in endpoints
        assert "heart_list" in endpoints
