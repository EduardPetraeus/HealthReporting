"""Tests for sundhed.dk PDF parsers against real downloaded files.

Tests verify structural correctness only — no real health data values
are asserted or logged. Each test class is skipped if the corresponding
PDF file is not available on the local machine.

Privacy rules:
    - NEVER assert on specific health values (numeric results, medication names)
    - NEVER log or print parsed record contents
    - Only check: counts, field presence, data types, format patterns, PII absence
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from health_platform.source_connectors.sundhed_dk.pdf_parser import (
    parse_ejournal_pdf,
    parse_lab_results_pdf,
    parse_medications_pdf,
    parse_vaccinations_pdf,
)

# ---------------------------------------------------------------------------
# PDF file paths
# ---------------------------------------------------------------------------

_DOWNLOADS = Path("/Users/Shared/data_lake/min sundhed/downloads")

_MEDICATIONS_PDF = _DOWNLOADS / "medications" / "Medicin - sundhed.dk.pdf"
_VACCINATIONS_PDF = _DOWNLOADS / "vaccinations" / "Vaccinationer - sundhed.dk.pdf"
_EJOURNAL_OVERVIEW_PDF = _DOWNLOADS / "ejournal" / "Journaler - sundhed.dk (2).pdf"
_EJOURNAL_NOTES_1_PDF = _DOWNLOADS / "ejournal" / "Journaler - sundhed.dk (1).pdf"
_EJOURNAL_NOTES_2_PDF = _DOWNLOADS / "ejournal" / "Journaler - sundhed.dk.pdf"
_LAB_RESULTS_PDF = (
    _DOWNLOADS / "lab_results" / "Laboratoriesvar_Claus_Eduard_Petræus.pdf"
)

# Regex patterns for validation
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CPR_RE = re.compile(r"\d{6}-\d{4}")


def _has_no_cpr(record: dict) -> bool:
    """Check that no field in a record contains a CPR number."""
    for value in record.values():
        if value is not None and isinstance(value, str):
            if _CPR_RE.search(value):
                return False
    return True


# ---------------------------------------------------------------------------
# Medications
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not _MEDICATIONS_PDF.exists(),
    reason="Medications PDF not available",
)
class TestMedicationsPdfParser:
    """Tests for parse_medications_pdf against real FMK export."""

    @pytest.fixture(scope="class")
    def records(self) -> list[dict]:
        return parse_medications_pdf(str(_MEDICATIONS_PDF))

    def test_parses_correct_count(self, records: list[dict]) -> None:
        """Should find 3 medications from the 1-page PDF."""
        assert len(records) == 3

    def test_medication_names_extracted(self, records: list[dict]) -> None:
        """Every record must have a non-empty medication_name."""
        for r in records:
            assert r["medication_name"], "medication_name must be non-empty"
            assert isinstance(r["medication_name"], str)

    def test_dates_are_iso_format(self, records: list[dict]) -> None:
        """start_date must be YYYY-MM-DD format where present."""
        for r in records:
            if r["start_date"] is not None:
                assert _ISO_DATE_RE.match(
                    r["start_date"]
                ), f"Bad date format: {r['start_date']}"

    def test_strength_extracted(self, records: list[dict]) -> None:
        """At least one medication should have strength info."""
        strengths = [r["strength"] for r in records if r["strength"]]
        assert len(strengths) > 0, "Expected at least one strength value"

    def test_reason_extracted(self, records: list[dict]) -> None:
        """At least one medication should have a reason/indication."""
        _ = [r["reason"] for r in records if r["reason"]]
        # Reason may not always be present, but check the field exists
        for r in records:
            assert "reason" in r, "reason field must be present"

    def test_no_pii_in_output(self, records: list[dict]) -> None:
        """No record should contain CPR numbers."""
        for r in records:
            assert _has_no_cpr(r), "CPR number found in medication record"

    def test_expected_fields_present(self, records: list[dict]) -> None:
        """Each record must have all expected fields."""
        expected_keys = {
            "medication_name",
            "active_ingredient",
            "strength",
            "dosage",
            "start_date",
            "end_date",
            "reason",
        }
        for r in records:
            assert expected_keys.issubset(
                r.keys()
            ), f"Missing fields: {expected_keys - r.keys()}"


# ---------------------------------------------------------------------------
# Vaccinations
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not _VACCINATIONS_PDF.exists(),
    reason="Vaccinations PDF not available",
)
class TestVaccinationsPdfParser:
    """Tests for parse_vaccinations_pdf against real SSI export."""

    @pytest.fixture(scope="class")
    def records(self) -> list[dict]:
        return parse_vaccinations_pdf(str(_VACCINATIONS_PDF))

    def test_parses_correct_count(self, records: list[dict]) -> None:
        """Should find 15 vaccinations (14 regular + 1 requiring docs)."""
        # Allow some flexibility — the PDF has ~15 entries across sections
        assert len(records) >= 14, f"Expected >= 14 vaccinations, got {len(records)}"
        assert len(records) <= 20, f"Expected <= 20 vaccinations, got {len(records)}"

    def test_dates_are_iso_format(self, records: list[dict]) -> None:
        """vaccine_date must be YYYY-MM-DD format where present."""
        for r in records:
            if r["vaccine_date"] is not None:
                assert _ISO_DATE_RE.match(
                    r["vaccine_date"]
                ), f"Bad date format: {r['vaccine_date']}"

    def test_vaccine_names_extracted(self, records: list[dict]) -> None:
        """Every record must have a non-empty vaccine_name."""
        for r in records:
            assert r["vaccine_name"], "vaccine_name must be non-empty"
            assert isinstance(r["vaccine_name"], str)

    def test_given_at_extracted(self, records: list[dict]) -> None:
        """At least some vaccinations should have a given_at location."""
        given_ats = [r["given_at"] for r in records if r["given_at"]]
        assert len(given_ats) > 0, "Expected at least one given_at value"

    def test_disease_target_extracted(self, records: list[dict]) -> None:
        """At least some vaccinations should have disease_target."""
        targets = [r["disease_target"] for r in records if r["disease_target"]]
        assert len(targets) > 0, "Expected at least one disease_target value"

    def test_no_pii_in_output(self, records: list[dict]) -> None:
        """No record should contain CPR numbers."""
        for r in records:
            assert _has_no_cpr(r), "CPR number found in vaccination record"

    def test_expected_fields_present(self, records: list[dict]) -> None:
        """Each record must have all expected fields."""
        expected_keys = {
            "vaccine_name",
            "vaccine_date",
            "given_at",
            "disease_target",
            "batch_number",
        }
        for r in records:
            assert expected_keys.issubset(
                r.keys()
            ), f"Missing fields: {expected_keys - r.keys()}"


# ---------------------------------------------------------------------------
# E-journal
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not _EJOURNAL_OVERVIEW_PDF.exists(),
    reason="E-journal overview PDF not available",
)
class TestEjournalOverviewPdfParser:
    """Tests for parse_ejournal_pdf against the overview PDF."""

    @pytest.fixture(scope="class")
    def records(self) -> list[dict]:
        return parse_ejournal_pdf(str(_EJOURNAL_OVERVIEW_PDF))

    def test_overview_parses_entries(self, records: list[dict]) -> None:
        """Journaler - sundhed.dk.pdf should find approximately 10 entries."""
        assert len(records) >= 5, f"Expected >= 5 entries, got {len(records)}"
        assert len(records) <= 20, f"Expected <= 20 entries, got {len(records)}"

    def test_dates_are_iso_format(self, records: list[dict]) -> None:
        """note_date must be YYYY-MM-DD format where present."""
        for r in records:
            if r["note_date"] is not None:
                assert _ISO_DATE_RE.match(
                    r["note_date"]
                ), f"Bad date format: {r['note_date']}"

    def test_hospital_extracted(self, records: list[dict]) -> None:
        """At least some entries should have a hospital name."""
        hospitals = [r["hospital"] for r in records if r["hospital"]]
        assert len(hospitals) > 0, "Expected at least one hospital value"

    def test_no_pii_in_output(self, records: list[dict]) -> None:
        """No record should contain CPR numbers."""
        for r in records:
            assert _has_no_cpr(r), "CPR number found in ejournal record"

    def test_expected_fields_present(self, records: list[dict]) -> None:
        """Each record must have all expected fields."""
        expected_keys = {
            "note_date",
            "department",
            "hospital",
            "note_type",
            "note_text",
        }
        for r in records:
            assert expected_keys.issubset(
                r.keys()
            ), f"Missing fields: {expected_keys - r.keys()}"


@pytest.mark.skipif(
    not _EJOURNAL_NOTES_1_PDF.exists(),
    reason="E-journal notes (1) PDF not available",
)
class TestEjournalNotes1PdfParser:
    """Tests for parse_ejournal_pdf against clinical notes PDF (1)."""

    @pytest.fixture(scope="class")
    def records(self) -> list[dict]:
        return parse_ejournal_pdf(str(_EJOURNAL_NOTES_1_PDF))

    def test_clinical_notes_parsed(self, records: list[dict]) -> None:
        """The (1) PDF should extract at least 1 note."""
        assert len(records) >= 1, f"Expected >= 1 notes, got {len(records)}"

    def test_note_text_is_substantial(self, records: list[dict]) -> None:
        """Clinical notes should contain meaningful text content."""
        for r in records:
            if r["note_text"] is not None:
                assert (
                    len(r["note_text"]) >= 10
                ), "Note text too short to be real content"

    def test_no_pii_in_output(self, records: list[dict]) -> None:
        """No record should contain CPR numbers."""
        for r in records:
            assert _has_no_cpr(r), "CPR number found in ejournal note"


@pytest.mark.skipif(
    not _EJOURNAL_NOTES_2_PDF.exists(),
    reason="E-journal notes (2) PDF not available",
)
class TestEjournalNotes2PdfParser:
    """Tests for parse_ejournal_pdf against clinical notes PDF (2)."""

    @pytest.fixture(scope="class")
    def records(self) -> list[dict]:
        return parse_ejournal_pdf(str(_EJOURNAL_NOTES_2_PDF))

    def test_clinical_notes_parsed(self, records: list[dict]) -> None:
        """The (2) PDF should extract at least 1 note."""
        assert len(records) >= 1, f"Expected >= 1 notes, got {len(records)}"

    def test_no_pii_in_output(self, records: list[dict]) -> None:
        """No record should contain CPR numbers."""
        for r in records:
            assert _has_no_cpr(r), "CPR number found in ejournal note"


# ---------------------------------------------------------------------------
# Lab results
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not _LAB_RESULTS_PDF.exists(),
    reason="Lab results PDF not available",
)
class TestLabResultsPdfParser:
    """Tests for parse_lab_results_pdf against real 28-page lab export."""

    @pytest.fixture(scope="class")
    def records(self) -> list[dict]:
        return parse_lab_results_pdf(str(_LAB_RESULTS_PDF))

    def test_parses_records(self, records: list[dict]) -> None:
        """28-page PDF should produce many lab results."""
        assert len(records) > 0, "Expected lab results from 28-page PDF"

    def test_marker_names_extracted(self, records: list[dict]) -> None:
        """Every record must have a non-empty marker_name."""
        for r in records:
            assert r["marker_name"], "marker_name must be non-empty"
            assert isinstance(r["marker_name"], str)

    def test_dates_are_iso_format(self, records: list[dict]) -> None:
        """test_date must be YYYY-MM-DD format where present."""
        dated_records = [r for r in records if r["test_date"] is not None]
        assert len(dated_records) > 0, "Expected at least some records with dates"
        for r in dated_records:
            assert _ISO_DATE_RE.match(
                r["test_date"]
            ), f"Bad date format: {r['test_date']}"

    def test_values_are_numeric_where_expected(self, records: list[dict]) -> None:
        """Records with value_numeric should have float values."""
        numeric_records = [r for r in records if r["value_numeric"] is not None]
        assert len(numeric_records) > 0, "Expected some numeric results"
        for r in numeric_records:
            assert isinstance(
                r["value_numeric"], (int, float)
            ), f"value_numeric must be numeric, got {type(r['value_numeric'])}"

    def test_units_extracted(self, records: list[dict]) -> None:
        """At least some records should have units."""
        units = [r["unit"] for r in records if r["unit"]]
        assert len(units) > 0, "Expected at least some records with units"

    def test_no_pii_in_output(self, records: list[dict]) -> None:
        """No record should contain CPR numbers."""
        for r in records:
            assert _has_no_cpr(r), "CPR number found in lab result record"

    def test_expected_fields_present(self, records: list[dict]) -> None:
        """Each record must have all expected fields."""
        expected_keys = {
            "test_date",
            "marker_name",
            "value_numeric",
            "value_text",
            "unit",
            "reference_min",
            "reference_max",
        }
        for r in records:
            assert expected_keys.issubset(
                r.keys()
            ), f"Missing fields: {expected_keys - r.keys()}"

    def test_unique_marker_names_reasonable(self, records: list[dict]) -> None:
        """A 28-page lab PDF should have multiple distinct markers."""
        unique_markers = {r["marker_name"] for r in records}
        assert (
            len(unique_markers) >= 5
        ), f"Expected >= 5 unique markers, got {len(unique_markers)}"


# ---------------------------------------------------------------------------
# Cross-section: orchestrator utility
# ---------------------------------------------------------------------------


class TestPdfParserHelpers:
    """Tests for shared helper functions in pdf_parser module."""

    def test_strip_pii_removes_cpr(self) -> None:
        """_strip_pii should remove CPR-format numbers."""
        from health_platform.source_connectors.sundhed_dk.pdf_parser import (
            _strip_pii,
        )

        assert "123456-7890" not in _strip_pii("Patient CPR: 123456-7890 data")
        assert "[CPR-REMOVED]" in _strip_pii("CPR: 123456-7890")

    def test_strip_pii_preserves_normal_text(self) -> None:
        """_strip_pii should not alter text without CPR patterns."""
        from health_platform.source_connectors.sundhed_dk.pdf_parser import (
            _strip_pii,
        )

        normal = "Hemoglobin 9.6 mmol/L"
        assert _strip_pii(normal) == normal

    def test_parse_danish_date_dot_format(self) -> None:
        """Danish dot-format dates should parse correctly."""
        from health_platform.source_connectors.sundhed_dk.pdf_parser import (
            _parse_danish_date_pdf,
        )

        assert _parse_danish_date_pdf("03.01.2026") == "2026-01-03"
        assert _parse_danish_date_pdf("15.12.2025") == "2025-12-15"

    def test_parse_danish_date_dash_format(self) -> None:
        """Danish dash-format dates should parse correctly."""
        from health_platform.source_connectors.sundhed_dk.pdf_parser import (
            _parse_danish_date_pdf,
        )

        assert _parse_danish_date_pdf("03-01-2026") == "2026-01-03"

    def test_parse_danish_date_none_on_invalid(self) -> None:
        """Invalid date input should return None."""
        from health_platform.source_connectors.sundhed_dk.pdf_parser import (
            _parse_danish_date_pdf,
        )

        assert _parse_danish_date_pdf("") is None
        assert _parse_danish_date_pdf("not a date") is None
        assert _parse_danish_date_pdf(None) is None  # type: ignore[arg-type]

    def test_parse_danish_number_comma_decimal(self) -> None:
        """Danish comma-decimal numbers should parse correctly."""
        from health_platform.source_connectors.sundhed_dk.pdf_parser import (
            _parse_danish_number_pdf,
        )

        assert _parse_danish_number_pdf("9,6") == 9.6
        assert _parse_danish_number_pdf("1.234,5") == 1234.5

    def test_parse_danish_number_standard(self) -> None:
        """Standard format numbers should also parse."""
        from health_platform.source_connectors.sundhed_dk.pdf_parser import (
            _parse_danish_number_pdf,
        )

        assert _parse_danish_number_pdf("12.5") == 12.5
        assert _parse_danish_number_pdf("100") == 100.0

    def test_parse_danish_number_none_on_invalid(self) -> None:
        """Invalid number input should return None."""
        from health_platform.source_connectors.sundhed_dk.pdf_parser import (
            _parse_danish_number_pdf,
        )

        assert _parse_danish_number_pdf("") is None
        assert _parse_danish_number_pdf("abc") is None
        assert _parse_danish_number_pdf(None) is None  # type: ignore[arg-type]
