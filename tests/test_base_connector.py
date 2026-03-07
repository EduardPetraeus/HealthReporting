"""Tests for BaseConnector ABC and OuraClient implementation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure imports resolve correctly
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))

from health_platform.source_connectors.base import BaseConnector
from health_platform.source_connectors.oura.client import OuraClient


class TestBaseConnectorABC:
    """Verify BaseConnector cannot be instantiated directly."""

    def test_cannot_instantiate(self):
        """BaseConnector is abstract and must not be instantiated."""
        with pytest.raises(TypeError):
            BaseConnector()  # type: ignore[abstract]

    def test_requires_all_abstract_methods(self):
        """A subclass that omits an abstract method cannot be instantiated."""

        class IncompleteConnector(BaseConnector):
            @property
            def source_name(self) -> str:
                return "incomplete"

            def authenticate(self) -> None:
                pass

            # Missing: fetch, get_endpoints

        with pytest.raises(TypeError):
            IncompleteConnector()  # type: ignore[abstract]


class TestOuraClientImplementsBaseConnector:
    """Verify OuraClient satisfies the BaseConnector interface."""

    @pytest.fixture
    def client(self):
        """OuraClient with a dummy token (no real API calls)."""
        return OuraClient(access_token="test-token-synthetic")

    def test_is_base_connector_subclass(self):
        assert issubclass(OuraClient, BaseConnector)

    def test_isinstance_check(self, client):
        assert isinstance(client, BaseConnector)

    def test_source_name_returns_oura(self, client):
        assert client.source_name == "oura"

    def test_get_endpoints_returns_expected(self, client):
        endpoints = client.get_endpoints()
        expected = [
            "daily_sleep",
            "daily_activity",
            "daily_readiness",
            "heartrate",
            "workout",
            "daily_spo2",
            "daily_stress",
        ]
        assert endpoints == expected

    def test_get_endpoints_returns_list_of_strings(self, client):
        endpoints = client.get_endpoints()
        assert isinstance(endpoints, list)
        assert all(isinstance(ep, str) for ep in endpoints)

    def test_authenticate_is_noop(self, client):
        """authenticate() should succeed without side effects."""
        client.authenticate()  # Should not raise

    def test_fetch_raises_on_unknown_endpoint(self, client):
        """fetch() with an invalid endpoint name should raise ValueError."""
        with pytest.raises(ValueError, match="Unknown endpoint"):
            client.fetch("nonexistent_endpoint", "2026-01-01", "2026-01-07")
