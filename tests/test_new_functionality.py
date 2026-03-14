"""Tests for new functionality in lab and genetics parsers.

Covers:
- Scientific notation parsing (_parse_scientific_notation)
- Below-detection-limit value handling (_extract_marker_from_row)
- Microbiome format detection (gettested_microbiome vs gettested)
- Ancestry percentage extraction (_parse_ancestry)
- Neanderthal variant parsing (_parse_neanderthal)
- Wellness traits dual output stream
- ValidationResult class (_approx_equal, _normalize_name)
- Format-to-test_type routing in lab_ingestion
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "health_unified_platform"),
)

from health_platform.quality.validate_lab_data import (  # noqa: E402
    ValidationResult,
    _approx_equal,
    _normalize_name,
)
from health_platform.source_connectors.genetics.pdf_parser import (  # noqa: E402
    GeneticsPdfParser,
)
from health_platform.source_connectors.lab.lab_ingestion import (  # noqa: E402
    _categorize_marker,
    _format_to_lab_name,
)
from health_platform.source_connectors.lab.pdf_parser import (  # noqa: E402
    LabPdfParser,
    _parse_danish_number,
    _parse_scientific_notation,
)

pytestmark = pytest.mark.integration


# =====================================================================
# Scientific notation parsing
# =====================================================================


class TestScientificNotationParsing:
    """Tests for _parse_scientific_notation — new in this diff."""

    def test_unicode_superscript_exponent(self):
        # "2.0 × 10⁶" — unicode multiplication sign + unicode superscript
        assert _parse_scientific_notation("2.0 × 10⁶") == 2_000_000.0

    def test_caret_exponent(self):
        # "2.0 x 10^6" — ascii x + caret notation
        assert _parse_scientific_notation("2.0 x 10^6") == 2_000_000.0

    def test_danish_coefficient_unicode_superscript(self):
        # "2,0 × 10⁶" — Danish decimal comma in coefficient
        assert _parse_scientific_notation("2,0 × 10⁶") == 2_000_000.0

    def test_danish_coefficient_caret(self):
        # "2,0 x 10^6"
        assert _parse_scientific_notation("2,0 x 10^6") == 2_000_000.0

    def test_exponent_4(self):
        assert _parse_scientific_notation("1.0 x 10^4") == 10_000.0

    def test_exponent_5_superscript(self):
        assert _parse_scientific_notation("5.0 × 10⁵") == 500_000.0

    def test_uppercase_X(self):
        # Capital X as multiplication sign
        assert _parse_scientific_notation("3.0 X 10^3") == 3_000.0

    def test_integer_coefficient(self):
        assert _parse_scientific_notation("2 x 10^6") == 2_000_000.0

    def test_plain_number_returns_none(self):
        # Plain numbers are not scientific notation
        assert _parse_scientific_notation("5.2") is None

    def test_empty_string_returns_none(self):
        assert _parse_scientific_notation("") is None

    def test_none_equivalent_returns_none(self):
        assert _parse_scientific_notation("   ") is None

    def test_random_text_returns_none(self):
        assert _parse_scientific_notation("Lactobacillus") is None

    def test_parse_danish_number_delegates_to_scientific(self):
        # _parse_danish_number should fall through to scientific notation
        result = _parse_danish_number("2.0 × 10⁶")
        assert result == 2_000_000.0

    def test_parse_danish_number_caret_via_fallback(self):
        result = _parse_danish_number("1.5 x 10^5")
        assert result == 150_000.0

    def test_superscript_zero(self):
        # 10⁰ = 1
        assert _parse_scientific_notation("3.0 × 10⁰") == 3.0

    def test_superscript_nine(self):
        assert _parse_scientific_notation("1.0 × 10⁹") == 1_000_000_000.0


# =====================================================================
# Below-detection-limit value handling
# =====================================================================


class TestBelowDetectionValues:
    """Tests for below-detection-limit parsing in _extract_marker_from_row."""

    def setup_method(self):
        self.parser = LabPdfParser()

    def _make_row(self, value_text: str) -> list:
        """Return a minimal table row for a below-detection value."""
        return ["Marker", value_text, "CFU/g", "10000 - 100000", ""]

    def _col_map(self) -> dict:
        return {"marker": 0, "value": 1, "unit": 2, "reference": 3, "status": 4}

    def test_below_detection_prefix_lt_with_scientific(self):
        row = self._make_row("< 1.0 x 10^4")
        result = self.parser._extract_marker_from_row(row, self._col_map())
        assert result is not None
        assert result["value_numeric"] is None
        assert result["value_text"] == "< 1.0 x 10^4"

    def test_below_detection_prefix_lt_plain_number(self):
        row = self._make_row("< 10000")
        result = self.parser._extract_marker_from_row(row, self._col_map())
        assert result is not None
        assert result["value_numeric"] is None
        assert result["value_text"] == "< 10000"

    def test_below_detection_prefix_with_unicode_times(self):
        row = self._make_row("< 1.0 × 10⁴")
        result = self.parser._extract_marker_from_row(row, self._col_map())
        assert result is not None
        assert result["value_numeric"] is None

    def test_normal_scientific_value_is_parsed(self):
        # A value WITHOUT "<" should parse to numeric via scientific notation
        row = ["Marker", "2.0 x 10^6", "CFU/g", "10000 - 100000", ""]
        result = self.parser._extract_marker_from_row(row, self._col_map())
        assert result is not None
        assert result["value_numeric"] == 2_000_000.0

    def test_below_detection_status_derived_from_reference(self):
        # Below-detection value → value_numeric is None → status = "unknown"
        row = self._make_row("< 1.0 x 10^4")
        result = self.parser._extract_marker_from_row(row, self._col_map())
        assert result["status"] == "unknown"


# =====================================================================
# Microbiome format detection
# =====================================================================


class TestMicrobiomeFormatDetection:
    """Tests for gettested_microbiome vs gettested format detection — new in diff."""

    def setup_method(self):
        self.parser = LabPdfParser()

    def test_detect_microbiome_by_keyword_mikrobiom(self):
        text = "GetTested Mikrobiom Analyse\nAfføringsprøve resultater"
        assert self.parser.detect_format(text) == "gettested_microbiome"

    def test_detect_microbiome_by_keyword_microbiome_english(self):
        text = "GetTested Microbiome stool test results"
        assert self.parser.detect_format(text) == "gettested_microbiome"

    def test_detect_microbiome_by_faeces(self):
        text = "Fæces analyse GetTested"
        assert self.parser.detect_format(text) == "gettested_microbiome"

    def test_detect_microbiome_by_feces_english(self):
        text = "Feces microbiome GetTested"
        assert self.parser.detect_format(text) == "gettested_microbiome"

    def test_detect_microbiome_by_stool_test(self):
        text = "stool test GetTested analysis"
        assert self.parser.detect_format(text) == "gettested_microbiome"

    def test_detect_regular_gettested_no_microbiome_keywords(self):
        # Regular blood panel — no microbiome keywords
        text = "GetTested BodyPanel Large Analysesvar blod"
        assert self.parser.detect_format(text) == "gettested"

    def test_microbiome_takes_priority_over_gettested(self):
        # Both microbiome and gettested keywords present — microbiome wins
        text = "GetTested Analysesvar Mikrobiom afføringsprøve"
        assert self.parser.detect_format(text) == "gettested_microbiome"

    def test_sundhed_dk_takes_priority_over_microbiome(self):
        # sundhed.dk must win even if microbiome keywords present
        text = "sundhed.dk min sundhed mikrobiom"
        assert self.parser.detect_format(text) == "sundhed_dk"


# =====================================================================
# Microbiome category mapping
# =====================================================================


class TestMicrobiomeCategoryMapping:
    """Tests for microbiome-specific categories added in this diff."""

    def test_aerobic_bacteria_ecoli(self):
        assert _categorize_marker("E. coli") == "aerobic_bacteria"

    def test_aerobic_bacteria_klebsiella(self):
        assert _categorize_marker("Klebsiella pneumoniae") == "aerobic_bacteria"

    def test_anaerobic_bacteria_bifidobacterium(self):
        assert _categorize_marker("Bifidobacterium spp.") == "anaerobic_bacteria"

    def test_anaerobic_bacteria_lactobacillus(self):
        assert _categorize_marker("Lactobacillus acidophilus") == "anaerobic_bacteria"

    def test_mycological_candida(self):
        assert _categorize_marker("Candida albicans") == "mycological"

    def test_gut_inflammation_calprotectin(self):
        assert _categorize_marker("Calprotectin") == "gut_inflammation"

    def test_gut_inflammation_alpha1_antitrypsin(self):
        assert _categorize_marker("Alpha-1 Antitrypsin") == "gut_inflammation"

    def test_digestive_function_pancreatic_elastase(self):
        assert _categorize_marker("Pancreatic Elastase") == "digestive_function"

    def test_immune_markers_secretory_iga(self):
        assert _categorize_marker("Secretory IgA") == "immune_markers"

    def test_gut_permeability_zonulin(self):
        assert _categorize_marker("Zonulin") == "gut_permeability"

    def test_stress_intolerance_histamin(self):
        assert _categorize_marker("Histamin") == "stress_intolerance"

    def test_format_to_lab_name_microbiome(self):
        assert _format_to_lab_name("gettested_microbiome") == "GetTested"

    def test_format_to_lab_name_gettested(self):
        assert _format_to_lab_name("gettested") == "GetTested"

    def test_format_to_lab_name_unknown_format(self):
        assert _format_to_lab_name("something_else") == "Unknown Lab"


# =====================================================================
# Ancestry percentage extraction
# =====================================================================


class TestAncestryPercentageExtraction:
    """Tests for GeneticsPdfParser._parse_ancestry — new in diff."""

    def setup_method(self):
        self.parser = GeneticsPdfParser()

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_ancestry_extracts_multiple_populations(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "Ancestry Composition\n"
            "Your ancestry at 50% confidence\n"
            "Scandinavian  84.4%\n"
            "Finnish  3.2%\n"
            "British & Irish  2.1%\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Ancestry Composition - 23andMe.pdf"))
        assert len(results) == 3
        pops = {r["population"] for r in results}
        assert "Scandinavian" in pops
        assert "Finnish" in pops
        assert "British & Irish" in pops

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_ancestry_percentages_correct(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = "Ancestry Composition\nYour ancestry at 50% confidence\nScandinavian  84.4%\n"
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Ancestry Composition - 23andMe.pdf"))
        assert len(results) == 1
        assert results[0]["percentage"] == 84.4

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_ancestry_confidence_level_extracted(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = "Ancestry Composition\nYour ancestry at 90% confidence\nScandinavian  84.4%\n"
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Ancestry Composition - 23andMe.pdf"))
        assert results[0]["confidence_level"] == "90%"

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_ancestry_region_mapped(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = "Ancestry Composition\nYour ancestry at 50% confidence\nScandinavian  84.4%\n"
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Ancestry Composition - 23andMe.pdf"))
        assert results[0]["region"] == "Northwestern European"

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_ancestry_unknown_population_maps_to_other(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "Ancestry Composition\nYour ancestry at 50% confidence\nAtlantean  1.0%\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Ancestry Composition - 23andMe.pdf"))
        assert results[0]["region"] == "Other"

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_ancestry_no_populations_returns_empty(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "Ancestry Composition\nNo population data available.\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Ancestry Composition - 23andMe.pdf"))
        assert results == []

    def test_extract_confidence_level_50(self):
        text = "Your ancestry at 50% confidence"
        assert GeneticsPdfParser._extract_confidence_level(text) == "50%"

    def test_extract_confidence_level_90(self):
        text = "results at 90% confidence level"
        assert GeneticsPdfParser._extract_confidence_level(text) == "90%"

    def test_extract_confidence_level_fallback(self):
        text = "No confidence level mentioned here"
        assert GeneticsPdfParser._extract_confidence_level(text) == "standard"


# =====================================================================
# Neanderthal variant parsing
# =====================================================================


class TestNeanderthalParsing:
    """Tests for GeneticsPdfParser._parse_neanderthal — new in diff."""

    def setup_method(self):
        self.parser = GeneticsPdfParser()

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_neanderthal_variant_count(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "Your Neanderthal Ancestry\n"
            "You have 312 Neanderthal variants.\n"
            "You have more variants than 96% of customers.\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Neanderthal - 23andMe.pdf"))
        assert len(results) == 1
        r = results[0]
        assert r["result_numeric"] == 312.0
        assert "312" in r["result_value"]

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_neanderthal_percentile_extracted(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "Your Neanderthal Ancestry\n"
            "You have 312 Neanderthal variants.\n"
            "You have more variants than 96% of customers.\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Neanderthal - 23andMe.pdf"))
        assert "96" in results[0]["result_value"]

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_neanderthal_category_is_neanderthal(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "Your Neanderthal Ancestry\nYou have 250 Neanderthal variants.\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Neanderthal - 23andMe.pdf"))
        assert results[0]["category"] == "neanderthal"
        assert results[0]["trait_name"] == "neanderthal_ancestry"

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_neanderthal_no_snp_or_gene(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "Your Neanderthal Ancestry\nYou have 200 Neanderthal variants.\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Neanderthal - 23andMe.pdf"))
        assert results[0]["gene"] is None
        assert results[0]["snp_id"] is None
        assert results[0]["genotype"] is None

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_neanderthal_only_count_no_percentile(self, mock_pdfplumber):
        # No percentile in text — variant count is still primary numeric
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "Your Neanderthal Ancestry\nYou have 150 Neanderthal variants.\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Neanderthal - 23andMe.pdf"))
        assert results[0]["result_numeric"] == 150.0

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_parse_neanderthal_ordinal_percentile(self, mock_pdfplumber):
        # "96th percentile" format
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "Your Neanderthal Ancestry\n"
            "You have 300 Neanderthal variants.\n"
            "96th percentile among 23andMe customers.\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Neanderthal - 23andMe.pdf"))
        # variant count is primary numeric (300), percentile appears in result_value
        assert results[0]["result_numeric"] == 300.0
        assert "96" in results[0]["result_value"]


# =====================================================================
# Wellness traits dual output
# =====================================================================


class TestWellnessTraitsParsing:
    """Tests for wellness report parsing via _parse_health_report."""

    def setup_method(self):
        self.parser = GeneticsPdfParser()

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_wellness_format_detected(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "Muscle Composition\n"
            "Test User, your genetic muscle composition is common.\n"
            "ACTN3 rs1815739 C T\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        self.parser.parse_pdf(Path("Muscle Composition - 23andMe.pdf"))
        assert self.parser.last_detected_format == "23andme_wellness"

    @patch("health_platform.source_connectors.genetics.pdf_parser.pdfplumber")
    def test_wellness_category_in_result(self, mock_pdfplumber):
        mock_page = MagicMock()
        mock_page.extract_text.return_value = (
            "Muscle Composition\n"
            "Test User, your genetic muscle composition is common.\n"
            "ACTN3 rs1815739 C T\n"
        )
        mock_pdf = MagicMock()
        mock_pdf.pages = [mock_page]
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_pdfplumber.open.return_value = mock_pdf

        results = self.parser.parse_pdf(Path("Muscle Composition - 23andMe.pdf"))
        assert len(results) == 1
        assert results[0]["category"] == "wellness"


# =====================================================================
# ValidationResult class
# =====================================================================


class TestValidationResult:
    """Tests for ValidationResult, _approx_equal, _normalize_name."""

    def test_add_pass(self):
        vr = ValidationResult("Test")
        vr.add("field1", "PASS", "expected", "actual")
        assert vr.pass_count == 1
        assert vr.warn_count == 0
        assert vr.fail_count == 0
        assert vr.total == 1

    def test_add_warn(self):
        vr = ValidationResult("Test")
        vr.add("field1", "WARN", "expected", "actual")
        assert vr.warn_count == 1

    def test_add_fail(self):
        vr = ValidationResult("Test")
        vr.add("field1", "FAIL", "expected", "actual")
        assert vr.fail_count == 1

    def test_mixed_statuses(self):
        vr = ValidationResult("Test")
        vr.add("f1", "PASS", "e", "a")
        vr.add("f2", "WARN", "e", "a")
        vr.add("f3", "FAIL", "e", "a")
        assert vr.pass_count == 1
        assert vr.warn_count == 1
        assert vr.fail_count == 1
        assert vr.total == 3

    def test_summary_contains_source_name(self):
        vr = ValidationResult("Blood Panel")
        vr.add("count", "PASS", "10", "10")
        summary = vr.summary()
        assert "Blood Panel" in summary

    def test_summary_shows_fail_detail(self):
        vr = ValidationResult("Test")
        vr.add("value", "FAIL", "5.0", "4.0")
        summary = vr.summary()
        assert "FAIL" in summary
        assert "5.0" in summary
        assert "4.0" in summary

    def test_summary_pass_no_detail(self):
        vr = ValidationResult("Test")
        vr.add("value", "PASS", "5.0", "5.0")
        summary = vr.summary()
        # Pass lines should NOT include expected/actual in detail
        assert "expected=5.0" not in summary

    def test_approx_equal_both_none(self):
        assert _approx_equal(None, None) is True

    def test_approx_equal_one_none(self):
        assert _approx_equal(5.0, None) is False
        assert _approx_equal(None, 5.0) is False

    def test_approx_equal_within_tolerance(self):
        assert _approx_equal(5.0, 5.005) is True

    def test_approx_equal_outside_tolerance(self):
        assert _approx_equal(5.0, 5.02) is False

    def test_approx_equal_custom_tolerance(self):
        assert _approx_equal(5.0, 5.05, tolerance=0.1) is True
        assert _approx_equal(5.0, 5.15, tolerance=0.1) is False

    def test_approx_equal_exact(self):
        assert _approx_equal(3.14, 3.14) is True

    def test_normalize_name_lowercase(self):
        assert _normalize_name("Cholesterol Total") == "cholesterol total"

    def test_normalize_name_strips_whitespace(self):
        assert _normalize_name("  HDL  ") == "hdl"

    def test_normalize_name_empty(self):
        assert _normalize_name("") == ""
