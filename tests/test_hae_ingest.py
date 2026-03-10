"""Tests for HAE (Health Auto Export) JSON → Parquet ingest pipeline.

Covers three layers:
1. parse_hae_payload() — JSON → flat records
2. write_hae_records() — records → partitioned Parquet
3. POST /v1/ingest/apple-health — API endpoint
Plus edge cases: malformed dates, null qty, duration, record cap, mapping integration.
"""

from __future__ import annotations

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_payload(
    metric_name: str = "step_count",
    unit: str = "count",
    qty: float = 1234.0,
    date: str = "2026-03-07 12:00:00 +0100",
    source: str = "Apple Watch",
    n_samples: int = 1,
) -> dict:
    """Build a minimal HAE JSON payload."""
    samples = [{"date": date, "qty": qty, "source": source}] * n_samples
    return {
        "data": {"metrics": [{"name": metric_name, "units": unit, "data": samples}]}
    }


def _make_multi_metric_payload() -> dict:
    """Payload with step_count + heart_rate."""
    return {
        "data": {
            "metrics": [
                {
                    "name": "step_count",
                    "units": "count",
                    "data": [
                        {
                            "date": "2026-03-07 08:00:00 +0100",
                            "qty": 500,
                            "source": "iPhone",
                        },
                        {
                            "date": "2026-03-07 12:00:00 +0100",
                            "qty": 800,
                            "source": "Apple Watch",
                        },
                    ],
                },
                {
                    "name": "heart_rate",
                    "units": "count/min",
                    "data": [
                        {
                            "date": "2026-03-07 10:00:00 +0100",
                            "qty": 72.0,
                            "source": "Apple Watch",
                        },
                    ],
                },
            ]
        }
    }


# ---------------------------------------------------------------------------
# 1. Parsing tests
# ---------------------------------------------------------------------------


class TestParseHaePayload:
    """Unit tests for parse_hae_payload()."""

    def test_single_metric(self):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
        )

        records = parse_hae_payload(_make_payload())
        assert len(records) == 1
        r = records[0]
        assert r["type"] == "HKQuantityTypeIdentifierStepCount"
        assert r["value"] == 1234.0
        assert r["sourceName"] == "Apple Watch"
        assert r["data_domain"] == "Activity"
        assert r["data_type"] == "stepcount"

    def test_multi_metric(self):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
        )

        records = parse_hae_payload(_make_multi_metric_payload())
        assert len(records) == 3
        types = {r["type"] for r in records}
        assert "HKQuantityTypeIdentifierStepCount" in types
        assert "HKQuantityTypeIdentifierHeartRate" in types

    def test_unknown_metric_skipped(self):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
        )

        payload = _make_payload(metric_name="totally_unknown_metric")
        records = parse_hae_payload(payload)
        assert len(records) == 0

    def test_empty_metrics_list(self):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
        )

        records = parse_hae_payload({"data": {"metrics": []}})
        assert records == []

    def test_missing_data_key(self):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
        )

        records = parse_hae_payload({})
        assert records == []

    def test_ingested_at_populated(self):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
        )

        records = parse_hae_payload(_make_payload())
        assert records[0]["_ingested_at"]
        assert "T" in records[0]["_ingested_at"]

    def test_end_date_equals_start_date(self):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
        )

        records = parse_hae_payload(_make_payload())
        assert records[0]["startDate"] == records[0]["endDate"]

    def test_body_mass_metric(self):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
        )

        payload = _make_payload(metric_name="body_mass", unit="kg", qty=80.5)
        records = parse_hae_payload(payload)
        assert len(records) == 1
        assert records[0]["data_domain"] == "BodyMetrics"
        assert records[0]["data_type"] == "bodymass"

    def test_mixed_known_and_unknown(self):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
        )

        payload = {
            "data": {
                "metrics": [
                    {
                        "name": "step_count",
                        "units": "count",
                        "data": [
                            {
                                "date": "2026-03-07 12:00:00 +0100",
                                "qty": 100,
                                "source": "iPhone",
                            }
                        ],
                    },
                    {
                        "name": "unknown_xyz",
                        "units": "?",
                        "data": [
                            {
                                "date": "2026-03-07 12:00:00 +0100",
                                "qty": 99,
                                "source": "?",
                            }
                        ],
                    },
                ]
            }
        }
        records = parse_hae_payload(payload)
        assert len(records) == 1
        assert records[0]["type"] == "HKQuantityTypeIdentifierStepCount"

    def test_multiple_samples_same_metric(self):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
        )

        payload = _make_payload(n_samples=5)
        records = parse_hae_payload(payload)
        assert len(records) == 5


# ---------------------------------------------------------------------------
# 2. Parquet write tests
# ---------------------------------------------------------------------------


class TestWriteHaeRecords:
    """Integration tests for write_hae_records()."""

    def test_creates_partitioned_parquet(self, tmp_path):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
            write_hae_records,
        )

        records = parse_hae_payload(_make_payload())
        result = write_hae_records(records, tmp_path)
        assert result["records_written"] == 1
        assert result["files_created"] == 1

        parquet_files = list(tmp_path.rglob("*.parquet"))
        assert len(parquet_files) == 1
        assert "Activity" in str(parquet_files[0])
        assert "stepcount" in str(parquet_files[0])
        assert "year=2026" in str(parquet_files[0])

    def test_multi_metric_creates_multiple_partitions(self, tmp_path):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
            write_hae_records,
        )

        records = parse_hae_payload(_make_multi_metric_payload())
        result = write_hae_records(records, tmp_path)
        assert result["records_written"] == 3
        assert result["files_created"] == 2

    def test_empty_records_noop(self, tmp_path):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            write_hae_records,
        )

        result = write_hae_records([], tmp_path)
        assert result == {"records_written": 0, "files_created": 0}

    def test_output_schema_matches_xml_export(self, tmp_path):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
            write_hae_records,
        )

        records = parse_hae_payload(_make_payload())
        write_hae_records(records, tmp_path)
        pf = list(tmp_path.rglob("*.parquet"))[0]
        df = pd.read_parquet(pf)
        expected_cols = {
            "type",
            "sourceName",
            "unit",
            "startDate",
            "endDate",
            "value",
            "data_domain",
            "data_type",
            "duration_seconds",
            "year",
            "_ingested_at",
        }
        assert expected_cols.issubset(set(df.columns))

    def test_parquet_filename_prefix(self, tmp_path):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
            write_hae_records,
        )

        records = parse_hae_payload(_make_payload())
        write_hae_records(records, tmp_path)
        pf = list(tmp_path.rglob("*.parquet"))[0]
        assert pf.name.startswith("hae_")

    def test_value_is_numeric(self, tmp_path):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
            write_hae_records,
        )

        records = parse_hae_payload(_make_payload())
        write_hae_records(records, tmp_path)
        pf = list(tmp_path.rglob("*.parquet"))[0]
        df = pd.read_parquet(pf)
        assert pd.api.types.is_numeric_dtype(df["value"])


# ---------------------------------------------------------------------------
# 3. API endpoint tests
# ---------------------------------------------------------------------------


@pytest.fixture
def _set_test_token(monkeypatch):
    """Patch auth module to use a known test token."""
    import health_platform.api.auth as auth_module

    monkeypatch.setattr(auth_module, "_cached_token", "test-token-12345")


@pytest.fixture
def client(tmp_path, _set_test_token, monkeypatch):
    """TestClient with tmp_path as DATA_LAKE_ROOT."""
    monkeypatch.setenv("DATA_LAKE_ROOT", str(tmp_path))
    from health_platform.api.server import app
    from starlette.testclient import TestClient

    return TestClient(app)


class TestIngestEndpoint:
    """API-level tests for POST /v1/ingest/apple-health."""

    def test_valid_payload_returns_200(self, client):
        resp = client.post(
            "/v1/ingest/apple-health",
            json=_make_payload(),
            headers={"Authorization": "Bearer test-token-12345"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["records_written"] == 1
        assert data["files_created"] == 1

    def test_no_auth_returns_401(self, client):
        resp = client.post("/v1/ingest/apple-health", json=_make_payload())
        assert resp.status_code == 401

    def test_wrong_token_returns_401(self, client):
        resp = client.post(
            "/v1/ingest/apple-health",
            json=_make_payload(),
            headers={"Authorization": "Bearer wrong-token"},
        )
        assert resp.status_code == 401

    def test_invalid_payload_returns_422(self, client):
        resp = client.post(
            "/v1/ingest/apple-health",
            json={"bad": "structure"},
            headers={"Authorization": "Bearer test-token-12345"},
        )
        assert resp.status_code == 422

    def test_empty_metrics_returns_200(self, client):
        resp = client.post(
            "/v1/ingest/apple-health",
            json={"data": {"metrics": []}},
            headers={"Authorization": "Bearer test-token-12345"},
        )
        assert resp.status_code == 200
        assert resp.json()["records_written"] == 0

    def test_unknown_only_returns_200_zero_records(self, client):
        payload = _make_payload(metric_name="nonexistent_metric")
        resp = client.post(
            "/v1/ingest/apple-health",
            json=payload,
            headers={"Authorization": "Bearer test-token-12345"},
        )
        assert resp.status_code == 200
        assert resp.json()["records_written"] == 0

    def test_parquet_written_to_data_lake(self, client, tmp_path):
        client.post(
            "/v1/ingest/apple-health",
            json=_make_payload(),
            headers={"Authorization": "Bearer test-token-12345"},
        )
        parquet_files = list((tmp_path / "apple_health_data").rglob("*.parquet"))
        assert len(parquet_files) >= 1


# ---------------------------------------------------------------------------
# 4. Edge case tests
# ---------------------------------------------------------------------------


class TestMalformedDates:
    """Verify graceful handling of bad date strings."""

    def test_empty_date_string_dropped(self, tmp_path):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
            write_hae_records,
        )

        payload = _make_payload(date="")
        records = parse_hae_payload(payload)
        result = write_hae_records(records, tmp_path)
        assert result["records_written"] == 0

    def test_garbage_date_dropped(self, tmp_path):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
            write_hae_records,
        )

        payload = _make_payload(date="not-a-date")
        records = parse_hae_payload(payload)
        result = write_hae_records(records, tmp_path)
        assert result["records_written"] == 0

    def test_mix_valid_and_invalid_dates(self, tmp_path):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            write_hae_records,
        )

        records = [
            {
                "type": "HKQuantityTypeIdentifierStepCount",
                "sourceName": "iPhone",
                "unit": "count",
                "startDate": "2026-03-07 12:00:00 +0100",
                "endDate": "2026-03-07 12:00:00 +0100",
                "value": 100,
                "data_domain": "Activity",
                "data_type": "stepcount",
                "_ingested_at": "20260307T120000Z",
            },
            {
                "type": "HKQuantityTypeIdentifierStepCount",
                "sourceName": "iPhone",
                "unit": "count",
                "startDate": "GARBAGE",
                "endDate": "GARBAGE",
                "value": 200,
                "data_domain": "Activity",
                "data_type": "stepcount",
                "_ingested_at": "20260307T120000Z",
            },
        ]
        result = write_hae_records(records, tmp_path)
        assert result["records_written"] == 1


class TestNullQty:
    """Verify handling of null/missing qty values."""

    def test_null_qty_produces_record(self):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
        )

        payload = _make_payload(qty=None)
        # Override to send explicit null
        payload["data"]["metrics"][0]["data"][0]["qty"] = None
        records = parse_hae_payload(payload)
        assert len(records) == 1
        assert records[0]["value"] is None

    def test_null_qty_coerced_to_nan_in_parquet(self, tmp_path):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
            write_hae_records,
        )

        payload = _make_payload()
        payload["data"]["metrics"][0]["data"][0]["qty"] = None
        records = parse_hae_payload(payload)
        write_hae_records(records, tmp_path)
        pf = list(tmp_path.rglob("*.parquet"))[0]
        df = pd.read_parquet(pf)
        assert pd.isna(df["value"].iloc[0])


class TestDuration:
    """HAE samples are point-in-time — duration_seconds should be 0."""

    def test_duration_is_zero(self, tmp_path):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
            write_hae_records,
        )

        records = parse_hae_payload(_make_payload())
        write_hae_records(records, tmp_path)
        pf = list(tmp_path.rglob("*.parquet"))[0]
        df = pd.read_parquet(pf)
        assert (df["duration_seconds"] == 0.0).all()


class TestRecordCap:
    """Verify MAX_RECORDS safety cap."""

    def test_cap_truncates_payload(self):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            MAX_RECORDS,
            parse_hae_payload,
        )

        payload = _make_payload(n_samples=MAX_RECORDS + 100)
        records = parse_hae_payload(payload)
        assert len(records) == MAX_RECORDS


class TestMappingIntegration:
    """Verify YAML mapping resolves correctly for key metrics."""

    @pytest.mark.parametrize(
        "metric,expected_domain,expected_type",
        [
            ("step_count", "Activity", "stepcount"),
            ("heart_rate", "Vitality", "heartrate"),
            ("body_mass", "BodyMetrics", "bodymass"),
            ("walking_speed", "Mobility", "walkingspeed"),
            ("vo2_max", "Vitality", "vo2max"),
        ],
    )
    def test_metric_domain_and_type(self, metric, expected_domain, expected_type):
        from health_platform.source_connectors.apple_health.json_to_parquet_writer import (
            parse_hae_payload,
        )

        payload = _make_payload(metric_name=metric, qty=42.0)
        records = parse_hae_payload(payload)
        assert len(records) == 1
        assert records[0]["data_domain"] == expected_domain
        assert records[0]["data_type"] == expected_type
