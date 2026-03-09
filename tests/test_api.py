"""Tests for the FastAPI health data server.

Uses FastAPI TestClient with in-memory DuckDB to test all endpoints.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))

from fastapi.testclient import TestClient
from health_platform.api.server import app


def _fake_keychain_run(*args, **kwargs):
    """Mock subprocess.run that simulates errSecItemNotFound (code 44)."""
    return subprocess.CompletedProcess(
        args=args[0] if args else [],
        returncode=44,
        stdout="",
        stderr="The specified item could not be found in the keychain.",
    )


@pytest.fixture(autouse=True)
def _set_test_token(monkeypatch):
    """Set a known API token for all tests."""
    monkeypatch.setenv("HEALTH_API_TOKEN", "test-token-12345")
    monkeypatch.setenv("HEALTH_ENV", "dev")
    # Bypass keychain — let get_secret fall back to env var
    monkeypatch.setattr(
        "health_platform.utils.keychain.subprocess.run",
        _fake_keychain_run,
    )
    # Clear cached token
    import health_platform.api.auth as auth_module

    auth_module._cached_token = None


@pytest.fixture
def api_db(tmp_path):
    """Create a temporary DuckDB with test data and patch the DB path."""
    db_file = tmp_path / "test_health.db"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")

    con.execute(
        """
        CREATE TABLE silver.daily_sleep (
            sk_date INTEGER, day DATE, sleep_score INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_sleep (day, sleep_score) VALUES
        ('2026-02-20', 82), ('2026-02-21', 75), ('2026-02-22', 91),
        ('2026-02-23', 68), ('2026-02-24', 85), ('2026-02-25', 79),
        ('2026-02-26', 88)
    """
    )

    con.execute(
        """
        CREATE TABLE silver.daily_readiness (
            sk_date INTEGER, day DATE, readiness_score INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_readiness (day, readiness_score) VALUES
        ('2026-02-20', 79), ('2026-02-21', 72), ('2026-02-22', 88),
        ('2026-02-23', 65), ('2026-02-24', 81), ('2026-02-25', 77),
        ('2026-02-26', 85)
    """
    )

    con.execute(
        """
        CREATE TABLE silver.daily_activity (
            sk_date INTEGER, day DATE, activity_score INTEGER, steps INTEGER,
            active_calories INTEGER, total_calories INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_activity (day, activity_score, steps) VALUES
        ('2026-02-20', 91, 12450), ('2026-02-21', 68, 5200),
        ('2026-02-22', 85, 9800)
    """
    )

    con.execute(
        """
        CREATE TABLE silver.daily_stress (
            sk_date INTEGER, day DATE, day_summary VARCHAR,
            stress_high INTEGER, recovery_high INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_stress (day, day_summary, stress_high, recovery_high) VALUES
        ('2026-02-20', 'restored', 120, 480)
    """
    )

    con.execute(
        """
        CREATE TABLE silver.workout (
            sk_date INTEGER, day DATE, workout_id VARCHAR, activity VARCHAR,
            intensity VARCHAR, calories INTEGER, distance_meters INTEGER,
            start_datetime TIMESTAMP, end_datetime TIMESTAMP,
            duration_seconds INTEGER, label VARCHAR, source VARCHAR,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.workout (day, activity, intensity, calories, duration_seconds) VALUES
        ('2026-02-20', 'running', 'high', 450, 1800)
    """
    )

    con.execute(
        """
        CREATE TABLE silver.weight (
            sk_date INTEGER, sk_time VARCHAR, datetime TIMESTAMP,
            weight_kg DOUBLE, fat_mass_kg DOUBLE, bone_mass_kg DOUBLE,
            muscle_mass_kg DOUBLE, hydration_kg DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )

    con.execute(
        """
        CREATE TABLE agent.patient_profile (
            profile_key VARCHAR, profile_value VARCHAR,
            numeric_value DOUBLE, category VARCHAR,
            description VARCHAR, computed_from VARCHAR,
            last_updated_at TIMESTAMP, update_frequency VARCHAR
        )
    """
    )
    con.execute(
        """
        INSERT INTO agent.patient_profile (category, profile_key, profile_value) VALUES
        ('demographics', 'age', '40'),
        ('demographics', 'biological_sex', 'male')
    """
    )

    con.execute(
        """
        CREATE TABLE agent.daily_summaries (
            day DATE, sleep_score INTEGER, readiness_score INTEGER,
            steps INTEGER, resting_hr DOUBLE, stress_level VARCHAR,
            has_anomaly BOOLEAN, anomaly_metrics VARCHAR,
            summary_text VARCHAR, embedding FLOAT[384],
            data_completeness DOUBLE, created_at TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO agent.daily_summaries (day, summary_text) VALUES
        ('2026-02-20', 'Good sleep quality with score 82. Active day.')
    """
    )

    con.execute(
        """
        CREATE TABLE agent.knowledge_base (
            insight_id VARCHAR PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            insight_type VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            content VARCHAR NOT NULL,
            evidence_query VARCHAR,
            confidence DOUBLE NOT NULL,
            tags VARCHAR[],
            embedding FLOAT[384],
            is_active BOOLEAN DEFAULT true,
            superseded_by VARCHAR
        )
    """
    )

    con.close()

    with patch.dict(os.environ, {"HEALTH_DB_PATH": str(db_file)}):
        yield str(db_file)


@pytest.fixture
def client(api_db):
    """TestClient with patched DB path."""
    return TestClient(app)


AUTH_HEADERS = {"Authorization": "Bearer test-token-12345"}


class TestHealthCheck:
    def test_health_no_auth(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_health_has_timestamp(self, client):
        response = client.get("/health")
        assert "timestamp" in response.json()


class TestAuthentication:
    def test_no_token_returns_401(self, client):
        response = client.post("/v1/chat", json={"question": "test"})
        assert response.status_code in (401, 403)

    def test_wrong_token_returns_401(self, client):
        response = client.post(
            "/v1/chat",
            json={"question": "test"},
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert response.status_code == 401

    def test_valid_token_works(self, client):
        response = client.post(
            "/v1/chat",
            json={"question": "how did I sleep?"},
            headers=AUTH_HEADERS,
        )
        assert response.status_code == 200


class TestQueryEndpoint:
    def test_query_sleep_score(self, client):
        response = client.get(
            "/v1/query",
            params={"metric": "sleep_score", "date_range": "2026-02-20:2026-02-26"},
            headers=AUTH_HEADERS,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["metric"] == "sleep_score"
        assert "82" in data["result"]

    def test_query_period_average(self, client):
        response = client.get(
            "/v1/query",
            params={
                "metric": "sleep_score",
                "date_range": "2026-02-20:2026-02-26",
                "computation": "period_average",
            },
            headers=AUTH_HEADERS,
        )
        assert response.status_code == 200
        data = response.json()
        assert "avg_sleep_score" in data["result"]

    def test_query_invalid_metric(self, client):
        response = client.get(
            "/v1/query",
            params={"metric": "nonexistent"},
            headers=AUTH_HEADERS,
        )
        assert response.status_code == 200
        data = response.json()
        assert "Error" in data["result"]


class TestProfileEndpoint:
    def test_get_full_profile(self, client):
        response = client.get("/v1/profile", headers=AUTH_HEADERS)
        assert response.status_code == 200
        data = response.json()
        assert "Patient Profile" in data["profile"]

    def test_get_filtered_profile(self, client):
        response = client.get(
            "/v1/profile",
            params={"categories": "demographics"},
            headers=AUTH_HEADERS,
        )
        assert response.status_code == 200
        data = response.json()
        assert "male" in data["profile"]


class TestAlertsEndpoint:
    def test_alerts_returns_data(self, client):
        response = client.get("/v1/alerts", headers=AUTH_HEADERS)
        assert response.status_code == 200
        data = response.json()
        assert "alerts" in data
        assert isinstance(data["alerts"], list)


class TestMobileSyncEndpoint:
    """Tests for GET /v1/mobile/sync — bulk data sync for mobile app."""

    def test_sync_returns_all_sections(self, client):
        response = client.get("/v1/mobile/sync", headers=AUTH_HEADERS)
        assert response.status_code == 200
        data = response.json()
        assert "metrics" in data
        assert "profile" in data
        assert "alerts" in data
        assert "daily_summaries" in data
        assert "sync_timestamp" in data

    def test_sync_contains_sleep_data(self, client):
        response = client.get("/v1/mobile/sync", headers=AUTH_HEADERS)
        data = response.json()
        assert "sleep_score" in data["metrics"]
        sleep_rows = data["metrics"]["sleep_score"]
        assert len(sleep_rows) == 7
        assert sleep_rows[0]["sleep_score"] == 82

    def test_sync_contains_steps_data(self, client):
        response = client.get("/v1/mobile/sync", headers=AUTH_HEADERS)
        data = response.json()
        assert "steps" in data["metrics"]
        step_rows = data["metrics"]["steps"]
        assert len(step_rows) == 3
        assert step_rows[0]["steps"] == 12450

    def test_sync_contains_profile(self, client):
        response = client.get("/v1/mobile/sync", headers=AUTH_HEADERS)
        data = response.json()
        profile = data["profile"]
        assert "demographics" in profile
        assert profile["demographics"]["biological_sex"] == "male"

    def test_sync_contains_daily_summaries(self, client):
        response = client.get("/v1/mobile/sync", headers=AUTH_HEADERS)
        data = response.json()
        summaries = data["daily_summaries"]
        assert len(summaries) >= 1
        assert "Good sleep" in summaries[0]["summary_text"]

    def test_sync_since_parameter_filters(self, client):
        response = client.get(
            "/v1/mobile/sync",
            params={"since": "2026-02-25T00:00:00"},
            headers=AUTH_HEADERS,
        )
        data = response.json()
        sleep_rows = data["metrics"]["sleep_score"]
        # Only 2026-02-25 and 2026-02-26 should remain
        assert len(sleep_rows) == 2

    def test_sync_requires_auth(self, client):
        response = client.get("/v1/mobile/sync")
        assert response.status_code in (401, 403)

    def test_sync_invalid_since_uses_default(self, client):
        response = client.get(
            "/v1/mobile/sync",
            params={"since": "not-a-date"},
            headers=AUTH_HEADERS,
        )
        assert response.status_code == 200
        data = response.json()
        assert "metrics" in data


class TestMobileThresholdsEndpoint:
    """Tests for GET /v1/mobile/thresholds — metric threshold definitions."""

    def test_thresholds_returns_data(self, client):
        response = client.get("/v1/mobile/thresholds", headers=AUTH_HEADERS)
        assert response.status_code == 200
        data = response.json()
        assert "thresholds" in data
        assert "metric_count" in data
        assert "timestamp" in data

    def test_thresholds_contains_sleep_score(self, client):
        response = client.get("/v1/mobile/thresholds", headers=AUTH_HEADERS)
        data = response.json()
        thresholds = data["thresholds"]
        assert "sleep_score" in thresholds
        assert "optimal" in thresholds["sleep_score"]
        assert thresholds["sleep_score"]["optimal"]["min"] == 85

    def test_thresholds_contains_steps(self, client):
        response = client.get("/v1/mobile/thresholds", headers=AUTH_HEADERS)
        data = response.json()
        thresholds = data["thresholds"]
        assert "steps" in thresholds
        assert thresholds["steps"]["optimal"]["min"] == 10000

    def test_thresholds_has_multiple_metrics(self, client):
        response = client.get("/v1/mobile/thresholds", headers=AUTH_HEADERS)
        data = response.json()
        assert data["metric_count"] >= 10

    def test_thresholds_requires_auth(self, client):
        response = client.get("/v1/mobile/thresholds")
        assert response.status_code in (401, 403)
