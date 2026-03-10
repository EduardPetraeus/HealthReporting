"""Tests for FHIR R4 resource mapping, bundle generation, and export endpoints.

All data is synthetic — no real health measurements used.
"""

from __future__ import annotations

import os
import subprocess
from datetime import date, datetime
from unittest.mock import patch

import duckdb
import pytest
from health_platform.export.fhir.bundle import FhirBundleGenerator
from health_platform.export.fhir.mapper import LOINC_CODES, FhirMapper

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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
    monkeypatch.setattr(
        "health_platform.utils.keychain.subprocess.run",
        _fake_keychain_run,
    )
    import health_platform.api.auth as auth_module

    auth_module._cached_token = None


@pytest.fixture
def export_db(tmp_path):
    """Create a temporary DuckDB with test data for export tests."""
    db_file = tmp_path / "test_export.db"
    con = duckdb.connect(str(db_file))
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    # Patient profile
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
        ('demographics', 'name', 'Test Patient'),
        ('demographics', 'biological_sex', 'male'),
        ('demographics', 'age', '45'),
        ('demographics', 'date_of_birth', '1981-03-15')
    """
    )

    # Daily sleep
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
        ('2026-03-01', 82), ('2026-03-02', 75), ('2026-03-03', 91)
    """
    )

    # Daily readiness
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
        ('2026-03-01', 79), ('2026-03-02', 72), ('2026-03-03', 88)
    """
    )

    # Daily activity
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
        ('2026-03-01', 91, 12450), ('2026-03-02', 68, 5200), ('2026-03-03', 85, 9800)
    """
    )

    # Daily SpO2
    con.execute(
        """
        CREATE TABLE silver.daily_spo2 (
            sk_date INTEGER, day DATE, spo2_avg_pct DOUBLE,
            breathing_disturbance_index DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    con.execute(
        """
        INSERT INTO silver.daily_spo2 (day, spo2_avg_pct) VALUES
        ('2026-03-01', 97.5), ('2026-03-02', 96.2), ('2026-03-03', 98.1)
    """
    )

    # Weight
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
        INSERT INTO silver.weight (datetime, weight_kg) VALUES
        ('2026-03-01 07:00:00', 82.5), ('2026-03-03 07:00:00', 82.3)
    """
    )

    # Daily summaries
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

    # Knowledge base (needed for server import)
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

    # Heart rate
    con.execute(
        """
        CREATE TABLE silver.heart_rate (
            sk_date INTEGER, sk_time VARCHAR,
            timestamp TIMESTAMP, bpm INTEGER, source_name VARCHAR,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )

    # Stress
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

    # Workout
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

    # Personal info (needed for some server imports)
    con.execute(
        """
        CREATE TABLE silver.personal_info (
            age INTEGER, weight_kg DOUBLE, height_m DOUBLE,
            biological_sex VARCHAR, email VARCHAR,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )

    con.close()

    with patch.dict(os.environ, {"HEALTH_DB_PATH": str(db_file)}):
        yield str(db_file)


@pytest.fixture
def mapper():
    """Fresh FhirMapper instance."""
    return FhirMapper(patient_id="test-patient-1")


# ---------------------------------------------------------------------------
# FhirMapper Tests
# ---------------------------------------------------------------------------


class TestMapPatientBasic:
    def test_map_patient_basic(self, mapper):
        profile = {
            "demographics": {
                "name": "Test Patient",
                "biological_sex": "male",
                "age": "45",
                "date_of_birth": "1981-03-15",
            }
        }
        patient = mapper.map_patient(profile)

        assert patient["resourceType"] == "Patient"
        assert patient["id"] == "test-patient-1"
        assert patient["gender"] == "male"
        assert patient["birthDate"] == "1981-03-15"
        assert patient["name"][0]["text"] == "Test Patient"

    def test_map_patient_empty_profile(self, mapper):
        patient = mapper.map_patient({})
        assert patient["resourceType"] == "Patient"
        assert patient["status"] == "unknown"
        assert "name" not in patient

    def test_map_patient_female(self, mapper):
        profile = {"demographics": {"biological_sex": "female"}}
        patient = mapper.map_patient(profile)
        assert patient["gender"] == "female"

    def test_map_patient_age_to_birth_year(self, mapper):
        profile = {"demographics": {"age": "30"}}
        patient = mapper.map_patient(profile)
        expected_year = str(date.today().year - 30)
        assert patient["birthDate"] == expected_year


class TestMapObservationVitalSign:
    def test_map_observation_vital_sign(self, mapper):
        obs = mapper.map_observation(
            metric_name="heart_rate",
            value=72,
            observation_date="2026-03-01",
            unit="/min",
        )

        assert obs["resourceType"] == "Observation"
        assert obs["status"] == "final"
        assert obs["code"]["coding"][0]["code"] == "8867-4"
        assert obs["valueQuantity"]["value"] == 72
        assert obs["valueQuantity"]["unit"] == "/min"
        assert obs["effectiveDateTime"] == "2026-03-01"
        assert obs["subject"]["reference"] == "Patient/test-patient-1"

    def test_map_observation_with_date_object(self, mapper):
        obs = mapper.map_observation(
            metric_name="weight",
            value=82.5,
            observation_date=date(2026, 3, 1),
        )
        assert obs["effectiveDateTime"] == "2026-03-01"
        assert obs["valueQuantity"]["value"] == 82.5

    def test_map_observation_with_datetime_object(self, mapper):
        dt = datetime(2026, 3, 1, 8, 30, 0)
        obs = mapper.map_observation("heart_rate", 65, dt)
        assert obs["effectiveDateTime"] == dt.isoformat()

    def test_map_observation_unknown_metric(self, mapper):
        obs = mapper.map_observation("unknown_metric", 42, "2026-03-01")
        assert obs["code"]["coding"][0]["code"] == "unknown"
        assert obs["code"]["coding"][0]["display"] == "Unknown Metric"

    def test_observation_has_category(self, mapper):
        obs = mapper.map_observation("heart_rate", 72, "2026-03-01")
        category = obs["category"][0]["coding"][0]
        assert category["code"] == "vital-signs"

    def test_survey_category_for_scores(self, mapper):
        obs = mapper.map_observation("sleep_score", 85, "2026-03-01")
        category = obs["category"][0]["coding"][0]
        assert category["code"] == "survey"


class TestMapVitalSignsFromRow:
    def test_map_vital_signs_from_row(self, mapper):
        row = {
            "day": "2026-03-01",
            "sleep_score": 82,
            "readiness_score": 79,
            "steps": 12450,
            "spo2_avg_pct": 97.5,
        }
        observations = mapper.map_vital_signs(row)

        assert len(observations) == 4
        resource_types = {obs["resourceType"] for obs in observations}
        assert resource_types == {"Observation"}

    def test_map_vital_signs_empty_row(self, mapper):
        observations = mapper.map_vital_signs({"day": "2026-03-01"})
        assert observations == []

    def test_map_vital_signs_partial_data(self, mapper):
        row = {"day": "2026-03-01", "weight_kg": 82.5}
        observations = mapper.map_vital_signs(row)
        assert len(observations) == 1
        assert observations[0]["code"]["coding"][0]["code"] == "29463-7"


class TestMapLabResult:
    def test_map_lab_result(self, mapper):
        lab = {
            "test_name": "Hemoglobin A1c",
            "value": 5.4,
            "unit": "%",
            "day": "2026-03-01",
            "reference_range_low": 4.0,
            "reference_range_high": 5.6,
        }
        obs = mapper.map_lab_result(lab)

        assert obs["resourceType"] == "Observation"
        assert obs["category"][0]["coding"][0]["code"] == "laboratory"
        assert obs["valueQuantity"]["value"] == 5.4
        assert obs["valueQuantity"]["unit"] == "%"
        assert len(obs["referenceRange"]) == 1
        assert obs["referenceRange"][0]["low"]["value"] == 4.0
        assert obs["referenceRange"][0]["high"]["value"] == 5.6

    def test_map_lab_result_no_reference_range(self, mapper):
        lab = {"test_name": "CRP", "value": 0.8, "unit": "mg/L", "day": "2026-03-01"}
        obs = mapper.map_lab_result(lab)
        assert "referenceRange" not in obs


class TestLoincCodesCorrect:
    def test_loinc_codes_correct(self):
        """Verify LOINC codes match expected values for common vital signs."""
        expected = {
            "heart_rate": "8867-4",
            "resting_heart_rate": "8867-4",
            "blood_pressure_systolic": "8480-6",
            "blood_pressure_diastolic": "8462-4",
            "spo2": "2708-6",
            "body_temperature": "8310-5",
            "respiratory_rate": "9279-1",
            "weight": "29463-7",
        }
        for metric, expected_code in expected.items():
            assert LOINC_CODES[metric]["code"] == expected_code, (
                f"LOINC code mismatch for {metric}: "
                f"expected {expected_code}, got {LOINC_CODES[metric]['code']}"
            )

    def test_all_loinc_entries_have_required_fields(self):
        for metric, entry in LOINC_CODES.items():
            assert "code" in entry, f"Missing 'code' for {metric}"
            assert "display" in entry, f"Missing 'display' for {metric}"
            assert "unit" in entry, f"Missing 'unit' for {metric}"
            assert "system" in entry, f"Missing 'system' for {metric}"


# ---------------------------------------------------------------------------
# FhirBundleGenerator Tests
# ---------------------------------------------------------------------------


class TestBundleGenerateEmpty:
    def test_generate_empty_bundle(self, export_db):
        """Bundle for a date range with no data should be empty but valid."""
        gen = FhirBundleGenerator(db_path=export_db)
        bundle = gen.generate_bundle(
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 31),
            resources=["Observation"],
        )
        assert bundle["resourceType"] == "Bundle"
        assert bundle["type"] == "collection"
        assert bundle["total"] == 0
        assert bundle["entry"] == []


class TestBundleStructureValid:
    def test_bundle_structure_valid(self, export_db):
        gen = FhirBundleGenerator(db_path=export_db)
        bundle = gen.generate_bundle(
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 3),
        )
        assert bundle["resourceType"] == "Bundle"
        assert bundle["type"] == "collection"
        assert "id" in bundle
        assert "timestamp" in bundle
        assert "total" in bundle
        assert isinstance(bundle["entry"], list)
        assert bundle["total"] == len(bundle["entry"])


class TestBundleContainsPatient:
    def test_bundle_contains_patient(self, export_db):
        gen = FhirBundleGenerator(db_path=export_db)
        bundle = gen.generate_bundle(
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 3),
            resources=["Patient"],
        )
        patient_entries = [
            e for e in bundle["entry"] if e["resource"]["resourceType"] == "Patient"
        ]
        assert len(patient_entries) == 1
        patient = patient_entries[0]["resource"]
        assert patient["gender"] == "male"

    def test_bundle_contains_observations(self, export_db):
        gen = FhirBundleGenerator(db_path=export_db)
        bundle = gen.generate_bundle(
            start_date=date(2026, 3, 1),
            end_date=date(2026, 3, 3),
        )
        obs_entries = [
            e for e in bundle["entry"] if e["resource"]["resourceType"] == "Observation"
        ]
        # Should have observations from sleep, readiness, activity, spo2, weight
        assert len(obs_entries) > 0


# ---------------------------------------------------------------------------
# Export API endpoint tests
# ---------------------------------------------------------------------------

AUTH_HEADERS = {"Authorization": "Bearer test-token-12345"}


class TestFhirEndpointRequiresAuth:
    def test_fhir_endpoint_requires_auth(self, export_db):
        from fastapi.testclient import TestClient
        from health_platform.api.server import app

        client = TestClient(app)
        response = client.get(
            "/v1/export/fhir",
            params={"start_date": "2026-03-01", "end_date": "2026-03-03"},
        )
        assert response.status_code in (401, 403)

    def test_fhir_endpoint_with_auth(self, export_db):
        from fastapi.testclient import TestClient
        from health_platform.api.server import app

        client = TestClient(app)
        response = client.get(
            "/v1/export/fhir",
            params={"start_date": "2026-03-01", "end_date": "2026-03-03"},
            headers=AUTH_HEADERS,
        )
        assert response.status_code == 200
        data = response.json()
        assert data["resourceType"] == "Bundle"
        assert data["type"] == "collection"

    def test_fhir_endpoint_invalid_date_range(self, export_db):
        from fastapi.testclient import TestClient
        from health_platform.api.server import app

        client = TestClient(app)
        response = client.get(
            "/v1/export/fhir",
            params={"start_date": "2026-03-05", "end_date": "2026-03-01"},
            headers=AUTH_HEADERS,
        )
        assert response.status_code == 400


class TestPdfEndpointRequiresAuth:
    def test_pdf_endpoint_requires_auth(self, export_db):
        from fastapi.testclient import TestClient
        from health_platform.api.server import app

        client = TestClient(app)
        response = client.get(
            "/v1/export/pdf",
            params={"start_date": "2026-03-01", "end_date": "2026-03-03"},
        )
        assert response.status_code in (401, 403)
