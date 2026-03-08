"""Tests for new Oura API V2 endpoints added in DS1 expansion."""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))

from health_platform.source_connectors.oura.client import (
    ENDPOINT_METHODS,
    OuraClient,
)

NEW_ENDPOINTS = [
    "daily_cardiovascular_age",
    "daily_resilience",
    "sleep_time",
    "enhanced_tag",
    "vo2_max",
    "session",
    "tag",
    "rest_mode_period",
    "ring_configuration",
    "sleep",
]


class TestNewEndpointRegistration:
    """Verify all new endpoints are registered in ENDPOINT_METHODS."""

    @pytest.mark.parametrize("endpoint", NEW_ENDPOINTS)
    def test_endpoint_in_methods_dict(self, endpoint):
        assert endpoint in ENDPOINT_METHODS

    @pytest.mark.parametrize("endpoint", NEW_ENDPOINTS)
    def test_endpoint_method_exists(self, endpoint):
        method_name = ENDPOINT_METHODS[endpoint]
        assert hasattr(OuraClient, method_name)

    def test_total_endpoint_count(self):
        assert len(ENDPOINT_METHODS) == 17

    def test_get_endpoints_returns_all(self):
        client = OuraClient(access_token="test-token")
        endpoints = client.get_endpoints()
        assert len(endpoints) == 17
        for ep in NEW_ENDPOINTS:
            assert ep in endpoints


class TestNewEndpointFetchDispatch:

    @pytest.fixture
    def client(self):
        return OuraClient(access_token="test-token")

    @pytest.mark.parametrize("endpoint", NEW_ENDPOINTS)
    def test_fetch_dispatches_to_method(self, client, endpoint):
        method_name = ENDPOINT_METHODS[endpoint]
        with patch.object(client, method_name, return_value=[]) as mock_method:
            result = client.fetch(endpoint, "2026-01-01", "2026-01-07")
            mock_method.assert_called_once()
            assert result == []


class TestNewEndpointURLPaths:

    @pytest.fixture
    def client(self):
        c = OuraClient(access_token="test-token")
        c._get_collection = MagicMock(return_value=[])
        return c

    @pytest.mark.parametrize(
        "endpoint,expected_path",
        [
            ("daily_cardiovascular_age", "/v2/usercollection/daily_cardiovascular_age"),
            ("daily_resilience", "/v2/usercollection/daily_resilience"),
            ("sleep_time", "/v2/usercollection/sleep_time"),
            ("enhanced_tag", "/v2/usercollection/enhanced_tag"),
            ("vo2_max", "/v2/usercollection/vo2_max"),
            ("session", "/v2/usercollection/session"),
            ("tag", "/v2/usercollection/tag"),
            ("rest_mode_period", "/v2/usercollection/rest_mode_period"),
            ("ring_configuration", "/v2/usercollection/ring_configuration"),
            ("sleep", "/v2/usercollection/sleep"),
        ],
    )
    def test_url_path(self, client, endpoint, expected_path):
        method_name = ENDPOINT_METHODS[endpoint]
        method = getattr(client, method_name)
        method(date(2026, 1, 1), date(2026, 1, 7))
        client._get_collection.assert_called_once()
        actual_path = client._get_collection.call_args[0][0]
        assert actual_path == expected_path

    @pytest.mark.parametrize("endpoint", NEW_ENDPOINTS)
    def test_uses_date_params(self, client, endpoint):
        method_name = ENDPOINT_METHODS[endpoint]
        method = getattr(client, method_name)
        method(date(2026, 1, 1), date(2026, 1, 7))
        params = client._get_collection.call_args[0][1]
        assert "start_date" in params
        assert "end_date" in params
        assert params["start_date"] == "2026-01-01"
        assert params["end_date"] == "2026-01-07"
