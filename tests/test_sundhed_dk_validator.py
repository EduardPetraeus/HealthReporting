"""Tests for the sundhed.dk validator module.

Covers count_expected_rows, compute_row_source_hash,
validate_parse_completeness, and generate_validation_report
with synthetic HTML fixtures matching the CSS selectors defined
in ROW_SELECTORS and GROUP_HEADER_SELECTORS.

No real health data — all fixtures are synthetic.
"""

from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))

from health_platform.source_connectors.sundhed_dk.validator import (  # noqa: E402
    ValidationReport,
    compute_row_source_hash,
    count_expected_rows,
    generate_validation_report,
    validate_parse_completeness,
)

# ---------------------------------------------------------------------------
# HTML fixture builders — all synthetic data
# ---------------------------------------------------------------------------

_LAB_HTML_3_ROWS = """
<div id="labsvargrid">
  <div class="ui-grid-pinned-container-left">
    <div class="ui-grid-viewport">
      <div class="ui-grid-row"><span class="group-header">Blodprøver 2030</span></div>
      <div class="ui-grid-row"><span>Syntetisk markør A</span></div>
      <div class="ui-grid-row"><span>Syntetisk markør B</span></div>
      <div class="ui-grid-row"><span>Syntetisk markør C</span></div>
    </div>
  </div>
</div>
"""

_LAB_HTML_ONLY_GROUP_HEADERS = """
<div id="labsvargrid">
  <div class="ui-grid-pinned-container-left">
    <div class="ui-grid-viewport">
      <div class="ui-grid-row"><span class="group-header">Gruppe 1</span></div>
      <div class="ui-grid-row"><span class="group-header">Gruppe 2</span></div>
    </div>
  </div>
</div>
"""

_LAB_HTML_WITH_EMPTY_ROW = """
<div class="ui-grid-pinned-container-left">
  <div class="ui-grid-viewport">
    <div class="ui-grid-row"><span>Reelt indhold</span></div>
    <div class="ui-grid-row">   </div>
  </div>
</div>
"""

_MEDICATIONS_HTML_2_ROWS = """
<table>
  <tbody>
    <tr class="sdk-table-row"><td>Testmedicin A</td></tr>
    <tr class="sdk-table-row"><td>Testmedicin B</td></tr>
  </tbody>
</table>
"""

_VACCINATIONS_HTML_3_ROWS = """
<table>
  <tbody>
    <tr class="sdk-table-row"><td>Fiktivvaccine MMR</td></tr>
    <tr class="sdk-table-row"><td>Influenza 2030</td></tr>
    <tr class="sdk-table-row"><td>Difteri-Tetanus</td></tr>
  </tbody>
</table>
"""

_EJOURNAL_HTML_2_ENTRIES = """
<div>
  <div class="journal-entry"><p>Konsultation 2030-01-10</p></div>
  <div class="journal-entry"><p>Konsultation 2030-02-05</p></div>
</div>
"""

_EJOURNAL_HTML_MIXED_SELECTORS = """
<div>
  <div class="journal-entry"><p>Notat via journal-entry</p></div>
  <div class="ejournal-item"><p>Notat via ejournal-item</p></div>
  <article class="note"><p>Notat via article.note</p></article>
</div>
"""

_APPOINTMENTS_HTML_2_ROWS = """
<datatable-body>
  <datatable-body-row><span>Fiktivt Hospital — 2030-03-01</span></datatable-body-row>
  <datatable-body-row><span>Eksempel Klinik — 2030-04-15</span></datatable-body-row>
</datatable-body>
"""


# ---------------------------------------------------------------------------
# count_expected_rows — lab_results
# ---------------------------------------------------------------------------


class TestCountExpectedRowsLabResults:
    """count_expected_rows for the lab_results section."""

    def test_counts_data_rows_and_excludes_group_header(self) -> None:
        assert count_expected_rows(_LAB_HTML_3_ROWS, "lab_results") == 3

    def test_only_group_headers_returns_zero(self) -> None:
        assert count_expected_rows(_LAB_HTML_ONLY_GROUP_HEADERS, "lab_results") == 0

    def test_empty_row_is_excluded(self) -> None:
        # Only 1 of the 2 rows has text content
        assert count_expected_rows(_LAB_HTML_WITH_EMPTY_ROW, "lab_results") == 1

    def test_empty_html_returns_zero(self) -> None:
        assert count_expected_rows("", "lab_results") == 0

    def test_whitespace_only_html_returns_zero(self) -> None:
        assert count_expected_rows("   \n\t  ", "lab_results") == 0

    def test_no_matching_structure_returns_zero(self) -> None:
        assert count_expected_rows("<div>Ingen grid her</div>", "lab_results") == 0


# ---------------------------------------------------------------------------
# count_expected_rows — medications
# ---------------------------------------------------------------------------


class TestCountExpectedRowsMedications:
    """count_expected_rows for the medications section."""

    def test_counts_sdk_table_rows(self) -> None:
        assert count_expected_rows(_MEDICATIONS_HTML_2_ROWS, "medications") == 2

    def test_empty_html_returns_zero(self) -> None:
        assert count_expected_rows("", "medications") == 0


# ---------------------------------------------------------------------------
# count_expected_rows — vaccinations
# ---------------------------------------------------------------------------


class TestCountExpectedRowsVaccinations:
    """count_expected_rows for the vaccinations section."""

    def test_counts_sdk_table_rows(self) -> None:
        assert count_expected_rows(_VACCINATIONS_HTML_3_ROWS, "vaccinations") == 3

    def test_empty_html_returns_zero(self) -> None:
        assert count_expected_rows("", "vaccinations") == 0


# ---------------------------------------------------------------------------
# count_expected_rows — ejournal
# ---------------------------------------------------------------------------


class TestCountExpectedRowsEjournal:
    """count_expected_rows for the ejournal section.

    ejournal uses three selectors: .journal-entry, .ejournal-item, article.note.
    All matching elements across all selectors are summed.
    """

    def test_counts_journal_entry_elements(self) -> None:
        assert count_expected_rows(_EJOURNAL_HTML_2_ENTRIES, "ejournal") == 2

    def test_counts_across_all_three_selectors(self) -> None:
        # 1 journal-entry + 1 ejournal-item + 1 article.note = 3
        assert count_expected_rows(_EJOURNAL_HTML_MIXED_SELECTORS, "ejournal") == 3

    def test_empty_html_returns_zero(self) -> None:
        assert count_expected_rows("", "ejournal") == 0


# ---------------------------------------------------------------------------
# count_expected_rows — appointments
# ---------------------------------------------------------------------------


class TestCountExpectedRowsAppointments:
    """count_expected_rows for the appointments section."""

    def test_counts_datatable_body_rows(self) -> None:
        assert count_expected_rows(_APPOINTMENTS_HTML_2_ROWS, "appointments") == 2

    def test_empty_html_returns_zero(self) -> None:
        assert count_expected_rows("", "appointments") == 0


# ---------------------------------------------------------------------------
# count_expected_rows — unknown section
# ---------------------------------------------------------------------------


class TestCountExpectedRowsUnknownSection:
    """count_expected_rows with an unrecognised section name."""

    def test_unknown_section_returns_zero(self) -> None:
        # No selectors defined → nothing matched → 0
        assert count_expected_rows(_MEDICATIONS_HTML_2_ROWS, "unknown_section") == 0


# ---------------------------------------------------------------------------
# compute_row_source_hash
# ---------------------------------------------------------------------------


class TestComputeRowSourceHash:
    """compute_row_source_hash — determinism, normalisation, return type."""

    def test_returns_32_char_hex_string(self) -> None:
        result = compute_row_source_hash("Syntetisk markør 7,2 mmol/L")
        assert len(result) == 32
        assert all(c in "0123456789abcdef" for c in result)

    def test_deterministic_same_input(self) -> None:
        content = "Syntetisk markør 7,2 mmol/L"
        assert compute_row_source_hash(content) == compute_row_source_hash(content)

    def test_different_input_different_hash(self) -> None:
        assert compute_row_source_hash("markør A") != compute_row_source_hash(
            "markør B"
        )

    def test_leading_trailing_whitespace_normalised(self) -> None:
        # Strip is applied before hashing
        assert compute_row_source_hash("  abc  ") == compute_row_source_hash("abc")

    def test_internal_whitespace_collapsed(self) -> None:
        # Multiple spaces / tabs / newlines collapsed to single space
        assert compute_row_source_hash("a   b\tc") == compute_row_source_hash("a b c")

    def test_newlines_normalised(self) -> None:
        assert compute_row_source_hash("line1\nline2") == compute_row_source_hash(
            "line1 line2"
        )

    def test_matches_manual_md5(self) -> None:
        content = "Syntetisk markør 7,2 mmol/L"
        normalized = re.sub(r"\s+", " ", content.strip())
        expected = hashlib.md5(normalized.encode("utf-8")).hexdigest()
        assert compute_row_source_hash(content) == expected

    def test_empty_string_returns_md5_of_empty(self) -> None:
        expected = hashlib.md5(b"").hexdigest()
        assert compute_row_source_hash("") == expected

    def test_unicode_content_handled(self) -> None:
        # Should not raise; Danish characters in health content
        result = compute_row_source_hash(
            "Glukose faste — 5,6 mmol/L — referenceinterval"
        )
        assert len(result) == 32


# ---------------------------------------------------------------------------
# validate_parse_completeness
# ---------------------------------------------------------------------------


class TestValidateParseCompleteness:
    """validate_parse_completeness — status, match flag, error messages."""

    def test_ok_when_counts_match(self) -> None:
        records = [{"marker": "A"}, {"marker": "B"}]
        report = validate_parse_completeness(
            _MEDICATIONS_HTML_2_ROWS, records, "medications"
        )
        assert report.status == "OK"
        assert report.match is True
        assert report.errors == []

    def test_html_rows_and_parsed_rows_populated(self) -> None:
        records = [{"marker": "A"}, {"marker": "B"}]
        report = validate_parse_completeness(
            _MEDICATIONS_HTML_2_ROWS, records, "medications"
        )
        assert report.html_rows == 2
        assert report.parsed_rows == 2

    def test_mismatch_when_fewer_records_parsed(self) -> None:
        records = [{"marker": "A"}]  # Only 1 of 2 parsed
        report = validate_parse_completeness(
            _MEDICATIONS_HTML_2_ROWS, records, "medications"
        )
        assert report.status == "MISMATCH"
        assert report.match is False
        assert len(report.errors) == 1

    def test_mismatch_error_message_mentions_delta(self) -> None:
        records = [{"marker": "A"}]
        report = validate_parse_completeness(
            _MEDICATIONS_HTML_2_ROWS, records, "medications"
        )
        assert "1" in report.errors[0]
        assert "fewer" in report.errors[0]

    def test_mismatch_when_more_records_than_html(self) -> None:
        # 1 row in HTML, 3 records → parser somehow produced more
        html = "<table><tbody><tr class='sdk-table-row'><td>X</td></tr></tbody></table>"
        records = [{"m": "A"}, {"m": "B"}, {"m": "C"}]
        report = validate_parse_completeness(html, records, "medications")
        assert report.status == "MISMATCH"
        assert "more" in report.errors[0]

    def test_empty_status_when_both_zero(self) -> None:
        report = validate_parse_completeness("", [], "lab_results")
        assert report.status == "EMPTY"
        assert report.match is True
        assert report.html_rows == 0
        assert report.parsed_rows == 0

    def test_empty_report_has_no_errors(self) -> None:
        report = validate_parse_completeness("", [], "vaccinations")
        assert report.errors == []

    def test_section_name_preserved(self) -> None:
        report = validate_parse_completeness("", [], "appointments")
        assert report.section == "appointments"

    def test_is_ok_true_when_ok(self) -> None:
        records = [{"r": 1}, {"r": 2}, {"r": 3}]
        report = validate_parse_completeness(
            _VACCINATIONS_HTML_3_ROWS, records, "vaccinations"
        )
        assert report.is_ok is True

    def test_is_ok_false_when_mismatch(self) -> None:
        report = validate_parse_completeness(
            _MEDICATIONS_HTML_2_ROWS, [], "medications"
        )
        assert report.is_ok is False

    def test_is_ok_false_for_empty_status(self) -> None:
        # EMPTY has match=True but status != "OK", so is_ok must be False
        report = validate_parse_completeness("", [], "lab_results")
        assert report.is_ok is False


# ---------------------------------------------------------------------------
# generate_validation_report
# ---------------------------------------------------------------------------


class TestGenerateValidationReport:
    """generate_validation_report — output format, summaries, error lines."""

    def _ok_report(self, section: str, n: int) -> ValidationReport:
        return ValidationReport(
            section=section,
            html_rows=n,
            parsed_rows=n,
            match=True,
            status="OK",
        )

    def _mismatch_report(self, section: str) -> ValidationReport:
        return ValidationReport(
            section=section,
            html_rows=5,
            parsed_rows=3,
            match=False,
            status="MISMATCH",
            errors=[
                "Row count mismatch: 3 parsed vs 5 in HTML (2 fewer records parsed)"
            ],
        )

    def test_output_is_string(self) -> None:
        report = generate_validation_report([self._ok_report("lab_results", 10)])
        assert isinstance(report, str)

    def test_header_row_present(self) -> None:
        output = generate_validation_report([self._ok_report("medications", 5)])
        assert "Section" in output
        assert "HTML Rows" in output
        assert "Parsed" in output
        assert "Status" in output

    def test_section_name_appears_in_output(self) -> None:
        output = generate_validation_report([self._ok_report("lab_results", 10)])
        assert "lab_results" in output

    def test_all_ok_summary_line(self) -> None:
        reports = [
            self._ok_report("lab_results", 10),
            self._ok_report("medications", 5),
        ]
        output = generate_validation_report(reports)
        assert "ALL SECTIONS OK" in output

    def test_mismatch_summary_line(self) -> None:
        output = generate_validation_report([self._mismatch_report("lab_results")])
        assert "VALIDATION ERRORS DETECTED" in output

    def test_mismatch_error_line_indented(self) -> None:
        output = generate_validation_report([self._mismatch_report("medications")])
        # Error lines are prefixed with "  ⚠"
        assert "  ⚠" in output

    def test_mismatch_error_content_present(self) -> None:
        output = generate_validation_report([self._mismatch_report("lab_results")])
        assert "fewer" in output

    def test_yes_match_string_for_ok(self) -> None:
        output = generate_validation_report([self._ok_report("vaccinations", 4)])
        assert "YES" in output

    def test_no_match_string_for_mismatch(self) -> None:
        output = generate_validation_report([self._mismatch_report("vaccinations")])
        assert "NO" in output

    def test_separator_lines_present(self) -> None:
        output = generate_validation_report([self._ok_report("appointments", 2)])
        assert "---" in output

    def test_mixed_ok_and_mismatch_reports(self) -> None:
        reports = [
            self._ok_report("medications", 3),
            self._mismatch_report("lab_results"),
        ]
        output = generate_validation_report(reports)
        assert "VALIDATION ERRORS DETECTED" in output
        assert "medications" in output
        assert "lab_results" in output

    def test_empty_report_list_produces_all_ok(self) -> None:
        output = generate_validation_report([])
        assert "ALL SECTIONS OK" in output

    def test_empty_section_status_appears(self) -> None:
        empty_report = ValidationReport(
            section="ejournal",
            html_rows=0,
            parsed_rows=0,
            match=True,
            status="EMPTY",
        )
        output = generate_validation_report([empty_report])
        assert "EMPTY" in output
        # EMPTY is not "OK" so summary should flag errors
        assert "VALIDATION ERRORS DETECTED" in output

    def test_row_counts_appear_in_output(self) -> None:
        output = generate_validation_report([self._ok_report("lab_results", 42)])
        assert "42" in output
