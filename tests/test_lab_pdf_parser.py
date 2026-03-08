"""Tests for the lab PDF parser, ingestion pipeline, and MCP tool.

All test data is synthetic — no real health data is used.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd

# Ensure the health_platform package is importable
sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "health_unified_platform"),
)

from health_platform.source_connectors.lab.pdf_parser import (
    LabPdfParser,
    _determine_status,
    _parse_danish_number,
    _parse_reference_range,
)
from health_platform.source_connectors.lab.lab_ingestion import (
    _categorize_marker,
    _compute_hash,
    _extract_date_from_filename,
    ingest_lab_pdfs,
)


# =====================================================================
# Danish number format tests
# =====================================================================


class TestDanishNumberParsing:
    """Test parsing of Danish-format numbers (comma as decimal separator)."""

    def test_standard_decimal(self):
        assert _parse_danish_number("12.5") == 12.5

    def test_danish_decimal_comma(self):
        assert _parse_danish_number("4,5") == 4.5

    def test_danish_with_thousands_dot(self):
        assert _parse_danish_number("1.234,5") == 1234.5

    def test_integer(self):
        assert _parse_danish_number("42") == 42.0

    def test_negative_danish(self):
        assert _parse_danish_number("-3,2") == -3.2

    def test_empty_string(self):
        assert _parse_danish_number("") is None

    def test_none_equivalent(self):
        assert _parse_danish_number("   ") is None

    def test_non_numeric(self):
        assert _parse_danish_number("abc") is None

    def test_zero_comma(self):
        assert _parse_danish_number("0,0") == 0.0

    def test_large_number_danish(self):
        assert _parse_danish_number("12.345,67") == 12345.67


# =====================================================================
# Reference range parsing tests
# =====================================================================


class TestReferenceRangeParsing:
    """Test parsing of reference range strings."""

    def test_dash_range_standard(self):
        ref_min, ref_max, direction = _parse_reference_range("3.5-5.0")
        assert ref_min == 3.5
        assert ref_max == 5.0
        assert direction is None

    def test_dash_range_danish(self):
        ref_min, ref_max, direction = _parse_reference_range("3,5 - 5,0")
        assert ref_min == 3.5
        assert ref_max == 5.0
        assert direction is None

    def test_en_dash_range(self):
        ref_min, ref_max, direction = _parse_reference_range("10\u201320")
        assert ref_min == 10.0
        assert ref_max == 20.0

    def test_em_dash_range(self):
        ref_min, ref_max, direction = _parse_reference_range("10\u201420")
        assert ref_min == 10.0
        assert ref_max == 20.0

    def test_less_than(self):
        ref_min, ref_max, direction = _parse_reference_range("< 50")
        assert ref_min is None
        assert ref_max == 50.0
        assert direction == "below"

    def test_greater_than(self):
        ref_min, ref_max, direction = _parse_reference_range("> 200")
        assert ref_min == 200.0
        assert ref_max is None
        assert direction == "above"

    def test_empty_range(self):
        ref_min, ref_max, direction = _parse_reference_range("")
        assert ref_min is None
        assert ref_max is None
        assert direction is None

    def test_none_range(self):
        ref_min, ref_max, direction = _parse_reference_range("")
        assert all(v is None for v in (ref_min, ref_max, direction))


# =====================================================================
# Status determination tests
# =====================================================================


class TestStatusDetermination:
    """Test status determination logic."""

    def test_normal_in_range(self):
        assert _determine_status(4.0, 3.5, 5.0, None) == "normal"

    def test_low_below_min(self):
        assert _determine_status(2.0, 3.5, 5.0, None) == "low"

    def test_high_above_max(self):
        assert _determine_status(6.0, 3.5, 5.0, None) == "high"

    def test_normal_at_boundary_min(self):
        assert _determine_status(3.5, 3.5, 5.0, None) == "normal"

    def test_normal_at_boundary_max(self):
        assert _determine_status(5.0, 3.5, 5.0, None) == "normal"

    def test_below_direction_normal(self):
        assert _determine_status(30.0, None, 50.0, "below") == "normal"

    def test_below_direction_high(self):
        assert _determine_status(60.0, None, 50.0, "below") == "high"

    def test_above_direction_normal(self):
        assert _determine_status(250.0, 200.0, None, "above") == "normal"

    def test_above_direction_low(self):
        assert _determine_status(150.0, 200.0, None, "above") == "low"

    def test_unknown_no_value(self):
        assert _determine_status(None, 3.5, 5.0, None) == "unknown"

    def test_unknown_no_reference(self):
        assert _determine_status(4.0, None, None, None) == "unknown"


# =====================================================================
# Format detection tests
# =====================================================================


class TestFormatDetection:
    """Test PDF format detection."""

    def setup_method(self):
        self.parser = LabPdfParser()

    def test_detect_gettested(self):
        text = "GetTested Analysesvar\nBodyPanel Large\nDato: 2026-01-15"
        assert self.parser.detect_format(text) == "gettested"

    def test_detect_gettested_lowercase(self):
        text = "blodprøve gettested resultat"
        assert self.parser.detect_format(text) == "gettested"

    def test_detect_sundhed_dk(self):
        text = "sundhed.dk\nMin Sundhed\nBlodprøvesvar"
        assert self.parser.detect_format(text) == "sundhed_dk"

    def test_detect_sundhed_dk_ejournal(self):
        text = "E-journal laboratoriesvar fra sundhed dk"
        assert self.parser.detect_format(text) == "sundhed_dk"

    def test_detect_unknown(self):
        text = "Random laboratory report from unknown lab"
        assert self.parser.detect_format(text) == "unknown"

    def test_detect_empty(self):
        assert self.parser.detect_format("") == "unknown"


# =====================================================================
# PDF parsing with mocked pdfplumber
# =====================================================================


class TestPdfParsing:
    """Test PDF parsing with mocked pdfplumber (no real PDFs needed)."""

    def setup_method(self):
        self.parser = LabPdfParser()

    def test_parse_gettested_table(self):
        """Test parsing a GetTested-style table."""
        tables = [
            [
                ["Analyse", "Resultat", "Enhed", "Reference", "Status"],
                ["Cholesterol, Total", "5,2", "mmol/L", "3,5 - 5,0", "Høj"],
                ["HDL Cholesterol", "1,8", "mmol/L", "1,0 - 2,5", "Normal"],
                ["LDL Cholesterol", "2,9", "mmol/L", "< 3,0", "Normal"],
                ["Hemoglobin", "145", "g/L", "130 - 170", "Normal"],
            ]
        ]

        markers = self.parser._parse_gettested(tables, "gettested bodypanel")
        assert len(markers) == 4

        chol = markers[0]
        assert chol["marker_name"] == "Cholesterol, Total"
        assert chol["value_numeric"] == 5.2
        assert chol["unit"] == "mmol/L"
        assert chol["reference_min"] == 3.5
        assert chol["reference_max"] == 5.0
        assert chol["status"] == "high"

        hdl = markers[1]
        assert hdl["value_numeric"] == 1.8
        assert hdl["status"] == "normal"

        ldl = markers[2]
        assert ldl["value_numeric"] == 2.9
        assert ldl["reference_max"] == 3.0
        assert ldl["reference_direction"] == "below"

    def test_parse_sundhed_dk_table(self):
        """Test parsing a sundhed.dk-style table."""
        tables = [
            [
                ["Analyse", "Svar", "Enhed", "Referenceinterval"],
                ["TSH", "2,1", "mU/L", "0,4 - 4,0"],
                ["Kreatinin", "85", "umol/L", "60 - 105"],
            ]
        ]

        markers = self.parser._parse_sundhed_dk(tables, "sundhed.dk test")
        assert len(markers) == 2

        tsh = markers[0]
        assert tsh["marker_name"] == "TSH"
        assert tsh["value_numeric"] == 2.1
        assert tsh["status"] == "normal"

    def test_parse_empty_table(self):
        """Test that empty tables are handled gracefully."""
        markers = self.parser._parse_gettested([], "")
        assert markers == []

    def test_parse_table_with_empty_rows(self):
        """Test handling of tables with empty/None rows."""
        tables = [
            [
                ["Analyse", "Resultat", "Enhed", "Reference"],
                None,
                ["", "", "", ""],
                ["Glucose", "5,1", "mmol/L", "3,9 - 5,6"],
            ]
        ]
        markers = self.parser._parse_gettested(tables, "gettested")
        assert len(markers) == 1
        assert markers[0]["marker_name"] == "Glucose"

    def test_parse_pdf_file_not_found(self):
        """Test that missing file returns empty list."""
        result = self.parser.parse_pdf(Path("/nonexistent/file.pdf"))
        assert result == []

    @patch("health_platform.source_connectors.lab.pdf_parser.pdfplumber")
    def test_parse_pdf_with_mock(self, mock_pdfplumber):
        """Test full parse_pdf flow with mocked pdfplumber."""
        # Create mock page with GetTested-style content
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "GetTested BodyPanel\nAnalyse Resultat Enhed Reference\nCRP 3,2 mg/L < 5"
        )
        mock_page.extract_tables.return_value = [
            [
                ["Analyse", "Resultat", "Enhed", "Reference"],
                ["CRP", "3,2", "mg/L", "< 5"],
            ]
        ]

        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(b"dummy")
            tmp_path = Path(f.name)

        try:
            markers = self.parser.parse_pdf(tmp_path)
            assert len(markers) == 1
            assert markers[0]["marker_name"] == "CRP"
            assert markers[0]["value_numeric"] == 3.2
            assert markers[0]["status"] == "normal"
        finally:
            tmp_path.unlink(missing_ok=True)


# =====================================================================
# Ingestion pipeline tests
# =====================================================================


class TestIngestionPipeline:
    """Test the lab PDF ingestion pipeline."""

    def test_extract_date_standard(self):
        assert _extract_date_from_filename("blood_test_2026-01-15.pdf") == "2026-01-15"

    def test_extract_date_compact(self):
        assert _extract_date_from_filename("20260115_gettested.pdf") == "2026-01-15"

    def test_extract_date_underscore_separated(self):
        assert _extract_date_from_filename("lab_results_2026_01_15.pdf") == "2026-01-15"

    def test_extract_date_no_date(self):
        assert _extract_date_from_filename("lab_report.pdf") is None

    def test_compute_hash_deterministic(self):
        h1 = _compute_hash("a", "b", "c")
        h2 = _compute_hash("a", "b", "c")
        assert h1 == h2

    def test_compute_hash_different(self):
        h1 = _compute_hash("a", "b")
        h2 = _compute_hash("a", "c")
        assert h1 != h2

    def test_categorize_marker_lipids(self):
        assert _categorize_marker("Total Cholesterol") == "lipids"
        assert _categorize_marker("LDL Cholesterol") == "lipids"
        assert _categorize_marker("HDL") == "lipids"

    def test_categorize_marker_thyroid(self):
        assert _categorize_marker("TSH") == "thyroid"
        assert _categorize_marker("Free T4") == "thyroid"

    def test_categorize_marker_vitamins(self):
        assert _categorize_marker("Vitamin D3") == "vitamins"
        assert _categorize_marker("B12 Cobalamin") == "vitamins"

    def test_categorize_marker_unknown(self):
        assert _categorize_marker("Some Random Marker XYZ") == "other"

    def test_categorize_marker_inflammation(self):
        assert _categorize_marker("CRP") == "inflammation"
        assert _categorize_marker("Calprotectin") == "inflammation"

    def test_categorize_marker_gut_health(self):
        assert _categorize_marker("Pancreatic Elastase") == "gut_health"
        assert _categorize_marker("Secretory IgA") == "gut_health"

    def test_categorize_marker_hba1c(self):
        """HbA1c is a glucose marker, not blood_count (H1 fix)."""
        assert _categorize_marker("Hemoglobin A1c") == "glucose"
        assert _categorize_marker("HbA1c") == "glucose"

    def test_categorize_marker_microalbuminuria(self):
        """Microalbuminuria is a kidney marker, not liver (H2 fix)."""
        assert _categorize_marker("Microalbuminuria") == "kidney"
        assert _categorize_marker("Microalbumin") == "kidney"

    def test_categorize_marker_plain_hemoglobin_still_blood_count(self):
        """Plain Hemoglobin (without A1c) should still be blood_count."""
        assert _categorize_marker("Hemoglobin") == "blood_count"
        assert _categorize_marker("Hæmoglobin") == "blood_count"

    def test_categorize_marker_plain_albumin_still_liver(self):
        """Plain Albumin (without micro prefix) should still be liver."""
        assert _categorize_marker("Albumin") == "liver"

    @patch("health_platform.source_connectors.lab.lab_ingestion.LabPdfParser")
    def test_ingest_writes_parquet(self, mock_parser_class):
        """Test that ingestion writes a valid parquet file."""
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser
        mock_parser.parse_pdf.return_value = [
            {
                "marker_name": "Glucose",
                "value_numeric": 5.1,
                "value_text": "5,1",
                "unit": "mmol/L",
                "reference_min": 3.9,
                "reference_max": 5.6,
                "reference_direction": None,
                "status": "normal",
            },
            {
                "marker_name": "HbA1c",
                "value_numeric": 38.0,
                "value_text": "38",
                "unit": "mmol/mol",
                "reference_min": None,
                "reference_max": 42.0,
                "reference_direction": "below",
                "status": "normal",
            },
        ]
        mock_parser.last_detected_format = "gettested"

        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_dir = Path(tmpdir) / "pdfs"
            pdf_dir.mkdir()
            output_dir = Path(tmpdir) / "output"

            # Create a dummy PDF file with date in name
            dummy_pdf = pdf_dir / "blood_test_2026-01-15.pdf"
            dummy_pdf.write_bytes(b"%PDF-1.4 dummy")

            result_path = ingest_lab_pdfs(pdf_dir, output_dir)

            assert result_path.exists()
            assert result_path.suffix == ".parquet"

            df = pd.read_parquet(result_path)
            assert len(df) == 2
            assert "test_id" in df.columns
            assert "business_key_hash" in df.columns
            assert "row_hash" in df.columns
            assert df.iloc[0]["marker_name"] == "Glucose"
            assert df.iloc[0]["test_date"] == "2026-01-15"
            assert df.iloc[1]["marker_name"] == "HbA1c"

    @patch("health_platform.source_connectors.lab.lab_ingestion.LabPdfParser")
    def test_ingest_empty_dir(self, mock_parser_class):
        """Test ingestion with no PDF files writes empty parquet."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pdf_dir = Path(tmpdir) / "pdfs"
            pdf_dir.mkdir()
            output_dir = Path(tmpdir) / "output"

            result_path = ingest_lab_pdfs(pdf_dir, output_dir)

            assert result_path.exists()
            df = pd.read_parquet(result_path)
            assert len(df) == 0


# =====================================================================
# MCP tool tests
# =====================================================================


class TestQueryLabResultsTool:
    """Test the query_lab_results MCP tool with in-memory DuckDB."""

    def setup_method(self):
        """Create an in-memory DuckDB with synthetic lab data."""
        self.con = duckdb.connect(":memory:")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        self.con.execute(
            """
            CREATE TABLE silver.lab_results (
                test_id             VARCHAR NOT NULL,
                test_date           DATE NOT NULL,
                test_type           VARCHAR NOT NULL,
                test_name           VARCHAR,
                lab_name            VARCHAR,
                lab_accreditation   VARCHAR,
                marker_name         VARCHAR NOT NULL,
                marker_category     VARCHAR NOT NULL,
                value_numeric       DOUBLE,
                value_text          VARCHAR,
                unit                VARCHAR,
                reference_min       DOUBLE,
                reference_max       DOUBLE,
                reference_direction VARCHAR,
                status              VARCHAR NOT NULL,
                business_key_hash   VARCHAR NOT NULL,
                row_hash            VARCHAR NOT NULL,
                load_datetime       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                update_datetime     TIMESTAMP
            )
        """
        )

        # Insert synthetic test data
        self.con.execute(
            """
            INSERT INTO silver.lab_results VALUES
            ('test-001', '2026-01-15', 'blood_panel', 'BodyPanel', 'GetTested', NULL,
             'Cholesterol Total', 'lipids', 5.2, '5,2', 'mmol/L', 3.5, 5.0, NULL,
             'high', 'hash1', 'rhash1', CURRENT_TIMESTAMP, NULL),
            ('test-001', '2026-01-15', 'blood_panel', 'BodyPanel', 'GetTested', NULL,
             'HDL Cholesterol', 'lipids', 1.8, '1,8', 'mmol/L', 1.0, 2.5, NULL,
             'normal', 'hash2', 'rhash2', CURRENT_TIMESTAMP, NULL),
            ('test-001', '2026-01-15', 'blood_panel', 'BodyPanel', 'GetTested', NULL,
             'Glucose', 'glucose', 5.1, '5,1', 'mmol/L', 3.9, 5.6, NULL,
             'normal', 'hash3', 'rhash3', CURRENT_TIMESTAMP, NULL),
            ('test-002', '2026-03-01', 'blood_panel', 'BodyPanel', 'GetTested', NULL,
             'Cholesterol Total', 'lipids', 4.8, '4,8', 'mmol/L', 3.5, 5.0, NULL,
             'normal', 'hash4', 'rhash4', CURRENT_TIMESTAMP, NULL),
            ('test-002', '2026-03-01', 'blood_panel', 'BodyPanel', 'GetTested', NULL,
             'Vitamin D3', 'vitamins', 35.0, '35', 'nmol/L', 50.0, 150.0, NULL,
             'low', 'hash5', 'rhash5', CURRENT_TIMESTAMP, NULL)
        """
        )

        from health_platform.mcp.health_tools import HealthTools

        self.tools = HealthTools(self.con)

    def teardown_method(self):
        self.con.close()

    def test_query_all(self):
        """Test querying all lab results."""
        result = self.tools.query_lab_results()
        assert "Cholesterol" in result
        assert "Glucose" in result
        assert "Vitamin D3" in result

    def test_query_by_marker_name(self):
        """Test filtering by marker name."""
        result = self.tools.query_lab_results(marker_name="cholesterol")
        assert "Cholesterol" in result
        # Should not include glucose
        assert "Glucose" not in result

    def test_query_by_status_high(self):
        """Test filtering by 'high' status."""
        result = self.tools.query_lab_results(status="high")
        assert "Cholesterol Total" in result
        assert "5.2" in result or "5,2" in result

    def test_query_by_status_low(self):
        """Test filtering by 'low' status."""
        result = self.tools.query_lab_results(status="low")
        assert "Vitamin D3" in result

    def test_query_by_status_normal(self):
        """Test filtering by 'normal' status."""
        result = self.tools.query_lab_results(status="normal")
        assert "HDL" in result
        assert "Glucose" in result

    def test_query_by_date_range(self):
        """Test filtering by date range."""
        result = self.tools.query_lab_results(date_range="2026-01-01:2026-01-31")
        assert "2026-01-15" in result
        assert "2026-03-01" not in result

    def test_query_combined_filters(self):
        """Test combining marker name and status filters."""
        result = self.tools.query_lab_results(
            marker_name="cholesterol", status="normal"
        )
        # Should match test-002 Cholesterol Total (normal) and HDL (normal)
        assert "normal" in result.lower() or "Normal" in result

    def test_query_no_results(self):
        """Test query with no matching results."""
        result = self.tools.query_lab_results(marker_name="nonexistent_marker")
        assert "No lab results found" in result

    def test_query_invalid_status(self):
        """Test query with invalid status value."""
        result = self.tools.query_lab_results(status="invalid")
        assert "Invalid status" in result

    def test_query_returns_markdown_table(self):
        """Test that results are formatted as a markdown table."""
        result = self.tools.query_lab_results()
        assert "|" in result  # Markdown table delimiter


# =====================================================================
# Generic table parsing tests
# =====================================================================


class TestGenericTableParsing:
    """Test the generic/fallback table parsing."""

    def setup_method(self):
        self.parser = LabPdfParser()

    def test_generic_parse_3_columns(self):
        """Test parsing a 3-column table (marker, value, reference)."""
        tables = [
            [
                ["Test", "Value", "Range"],
                ["Hemoglobin", "145", "130-170"],
            ]
        ]
        markers = self.parser._parse_generic_tables(tables)
        assert len(markers) == 1
        assert markers[0]["marker_name"] == "Hemoglobin"
        assert markers[0]["value_numeric"] == 145.0

    def test_generic_parse_5_columns(self):
        """Test parsing a 5-column table with status."""
        tables = [
            [
                ["Test", "Value", "Unit", "Range", "Status"],
                ["CRP", "3.2", "mg/L", "< 5", "Normal"],
            ]
        ]
        markers = self.parser._parse_generic_tables(tables)
        assert len(markers) == 1
        assert markers[0]["marker_name"] == "CRP"
        assert markers[0]["status"] == "normal"

    def test_generic_skip_short_table(self):
        """Test that tables with < 3 columns are skipped."""
        tables = [
            [
                ["A", "B"],
                ["X", "Y"],
            ]
        ]
        markers = self.parser._parse_generic_tables(tables)
        assert markers == []
