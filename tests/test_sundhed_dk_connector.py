"""Tests for the sundhed.dk connector parsers.

Tests pure HTML parsing functions with synthetic fixtures that match
the actual sundhed.dk HTML structure (AngularJS sdk-table, ui-grid,
ngx-datatable). No browser or network dependency — fully offline.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

# Connector root paths
_REPO_ROOT = Path(__file__).resolve().parents[1]
_SUNDHED_DIR = (
    _REPO_ROOT
    / "health_unified_platform"
    / "health_platform"
    / "source_connectors"
    / "sundhed_dk"
)
_PLATFORM_DIR = _REPO_ROOT / "health_unified_platform" / "health_platform"
_FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "sundhed_dk"


def _load_module(module_name: str, file_path: Path):
    """Load a module from a specific file path to avoid name collisions."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# Load the parsers module
parsers = _load_module("sundhed_dk_parsers", _SUNDHED_DIR / "parsers.py")


def _read_fixture(name: str) -> str:
    """Read a fixture HTML file."""
    return (_FIXTURES_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Danish date parser tests
# ---------------------------------------------------------------------------


class TestParseDanishDate:
    """Tests for the _parse_danish_date helper."""

    def test_dash_format(self) -> None:
        assert parsers._parse_danish_date("03-01-2026") == "2026-01-03"

    def test_dot_format(self) -> None:
        assert parsers._parse_danish_date("15.02.2026") == "2026-02-15"

    def test_named_month(self) -> None:
        assert parsers._parse_danish_date("5. marts 2021") == "2021-03-05"

    def test_named_month_januar(self) -> None:
        assert parsers._parse_danish_date("3. januar 2020") == "2020-01-03"

    def test_empty_string(self) -> None:
        assert parsers._parse_danish_date("") is None

    def test_none_input(self) -> None:
        assert parsers._parse_danish_date(None) is None

    def test_garbage_input(self) -> None:
        assert parsers._parse_danish_date("not a date") is None


# ---------------------------------------------------------------------------
# Lab results parser tests (Angular UI Grid)
# ---------------------------------------------------------------------------


class TestParseLabResults:
    """Tests for parse_lab_results with Angular UI Grid fixture."""

    def test_parses_marker_rows(self) -> None:
        html = _read_fixture("lab_results.html")
        results = parsers.parse_lab_results(html)
        # 3 data markers (group header is skipped)
        assert len(results) == 3

    def test_marker_names(self) -> None:
        html = _read_fixture("lab_results.html")
        results = parsers.parse_lab_results(html)
        names = [r["marker_name"] for r in results]
        assert "Testmarkør Alpha" in names
        assert "Testmarkør Beta" in names
        assert "Testmarkør Gamma" in names

    def test_danish_number_parsing(self) -> None:
        html = _read_fixture("lab_results.html")
        results = parsers.parse_lab_results(html)
        alpha = next(r for r in results if r["marker_name"] == "Testmarkør Alpha")
        assert alpha["value_numeric"] == 7.2

    def test_units_extracted(self) -> None:
        html = _read_fixture("lab_results.html")
        results = parsers.parse_lab_results(html)
        alpha = next(r for r in results if r["marker_name"] == "Testmarkør Alpha")
        assert alpha["unit"] == "mmol/L"

    def test_date_from_header(self) -> None:
        html = _read_fixture("lab_results.html")
        results = parsers.parse_lab_results(html)
        alpha = next(r for r in results if r["marker_name"] == "Testmarkør Alpha")
        assert alpha["test_date"] == "2030-06-01"

    def test_empty_html(self) -> None:
        assert parsers.parse_lab_results("") == []

    def test_no_grid(self) -> None:
        assert parsers.parse_lab_results("<div>No grid here</div>") == []


# ---------------------------------------------------------------------------
# Medications parser tests (sdk-table)
# ---------------------------------------------------------------------------


class TestParseMedications:
    """Tests for parse_medications with sdk-table fixture."""

    def test_parses_all_medications(self) -> None:
        html = _read_fixture("medications.html")
        results = parsers.parse_medications(html)
        assert len(results) == 3

    def test_medication_names(self) -> None:
        html = _read_fixture("medications.html")
        results = parsers.parse_medications(html)
        names = [r["medication_name"] for r in results]
        assert "Testmedicin" in names
        assert "Demopille" in names
        assert "Placebomax" in names

    def test_active_ingredient(self) -> None:
        html = _read_fixture("medications.html")
        results = parsers.parse_medications(html)
        med = next(r for r in results if r["medication_name"] == "Testmedicin")
        assert med["active_ingredient"] == "Fiktivstof"

    def test_strength_from_second_line(self) -> None:
        html = _read_fixture("medications.html")
        results = parsers.parse_medications(html)
        med = next(r for r in results if r["medication_name"] == "Testmedicin")
        assert "100 mg" in med["strength"]

    def test_start_date_parsed(self) -> None:
        html = _read_fixture("medications.html")
        results = parsers.parse_medications(html)
        med = next(r for r in results if r["medication_name"] == "Testmedicin")
        assert med["start_date"] == "2030-06-01"

    def test_dosage(self) -> None:
        html = _read_fixture("medications.html")
        results = parsers.parse_medications(html)
        med = next(r for r in results if r["medication_name"] == "Testmedicin")
        assert med["dosage"] == "1 tablet dagligt"

    def test_reason(self) -> None:
        html = _read_fixture("medications.html")
        results = parsers.parse_medications(html)
        med = next(r for r in results if r["medication_name"] == "Testmedicin")
        assert med["reason"] == "Til testformaal"

    def test_empty_reason(self) -> None:
        html = _read_fixture("medications.html")
        results = parsers.parse_medications(html)
        placebo = next(r for r in results if r["medication_name"] == "Placebomax")
        assert placebo["reason"] is None

    def test_end_date_not_in_table(self) -> None:
        html = _read_fixture("medications.html")
        results = parsers.parse_medications(html)
        assert all(r["end_date"] is None for r in results)

    def test_atc_code_not_in_table(self) -> None:
        html = _read_fixture("medications.html")
        results = parsers.parse_medications(html)
        assert all(r["atc_code"] is None for r in results)

    def test_empty_html(self) -> None:
        assert parsers.parse_medications("") == []


# ---------------------------------------------------------------------------
# Vaccinations parser tests (sdk-table effectuated-vaccinations)
# ---------------------------------------------------------------------------


class TestParseVaccinations:
    """Tests for parse_vaccinations with sdk-table fixture."""

    def test_parses_all_vaccinations(self) -> None:
        html = _read_fixture("vaccinations.html")
        results = parsers.parse_vaccinations(html)
        assert len(results) == 4

    def test_vaccine_names(self) -> None:
        html = _read_fixture("vaccinations.html")
        results = parsers.parse_vaccinations(html)
        names = [r["vaccine_name"] for r in results]
        assert any("Fiktivvirus" in n for n in names)
        assert any("Influenza" in n for n in names)
        assert any("Difteri" in n for n in names)

    def test_date_dot_format(self) -> None:
        html = _read_fixture("vaccinations.html")
        results = parsers.parse_vaccinations(html)
        assert results[0]["vaccine_date"] == "2030-04-01"

    def test_given_at(self) -> None:
        html = _read_fixture("vaccinations.html")
        results = parsers.parse_vaccinations(html)
        assert results[0]["given_at"] == "Fiktivt Vaccinationscenter"

    def test_given_at_none_when_empty(self) -> None:
        html = _read_fixture("vaccinations.html")
        results = parsers.parse_vaccinations(html)
        last = results[3]
        assert last["given_at"] is None

    def test_duration(self) -> None:
        html = _read_fixture("vaccinations.html")
        results = parsers.parse_vaccinations(html)
        flu = next(r for r in results if "Influenza" in r["vaccine_name"])
        assert flu["duration"] == "12 mdr."

    def test_batch_number_not_in_table(self) -> None:
        html = _read_fixture("vaccinations.html")
        results = parsers.parse_vaccinations(html)
        assert all(r["batch_number"] is None for r in results)

    def test_empty_html(self) -> None:
        assert parsers.parse_vaccinations("") == []


# ---------------------------------------------------------------------------
# E-journal parser tests (placeholder — needs correct URL dump)
# ---------------------------------------------------------------------------


class TestParseEjournal:
    """Tests for parse_ejournal — placeholder until correct URL is scraped."""

    def test_empty_html(self) -> None:
        assert parsers.parse_ejournal("") == []

    def test_no_matching_elements(self) -> None:
        assert parsers.parse_ejournal("<div>Dashboard only</div>") == []


# ---------------------------------------------------------------------------
# Appointments parser tests (ngx-datatable)
# ---------------------------------------------------------------------------


class TestParseAppointments:
    """Tests for parse_appointments with ngx-datatable fixture."""

    def test_parses_active_and_previous(self) -> None:
        html = _read_fixture("appointments.html")
        results = parsers.parse_appointments(html)
        assert len(results) == 2

    def test_active_section(self) -> None:
        html = _read_fixture("appointments.html")
        results = parsers.parse_appointments(html)
        active = [r for r in results if r["section"] == "active"]
        assert len(active) == 1

    def test_previous_section(self) -> None:
        html = _read_fixture("appointments.html")
        results = parsers.parse_appointments(html)
        previous = [r for r in results if r["section"] == "previous"]
        assert len(previous) == 1

    def test_referral_date(self) -> None:
        html = _read_fixture("appointments.html")
        results = parsers.parse_appointments(html)
        active = next(r for r in results if r["section"] == "active")
        assert active["referral_date"] == "2030-05-01"

    def test_expiry_date(self) -> None:
        html = _read_fixture("appointments.html")
        results = parsers.parse_appointments(html)
        active = next(r for r in results if r["section"] == "active")
        assert active["expiry_date"] == "2030-11-01"

    def test_referring_clinic(self) -> None:
        html = _read_fixture("appointments.html")
        results = parsers.parse_appointments(html)
        active = next(r for r in results if r["section"] == "active")
        assert active["referring_clinic"] == "Fiktiv Laegepraksis"

    def test_receiving_clinic(self) -> None:
        html = _read_fixture("appointments.html")
        results = parsers.parse_appointments(html)
        active = next(r for r in results if r["section"] == "active")
        assert active["receiving_clinic"] == "Eksempel Hospital"

    def test_specialty(self) -> None:
        html = _read_fixture("appointments.html")
        results = parsers.parse_appointments(html)
        active = next(r for r in results if r["section"] == "active")
        assert active["specialty"] == "Testspeciale"

    def test_empty_html(self) -> None:
        assert parsers.parse_appointments("") == []


# ---------------------------------------------------------------------------
# Scraper unit tests (no browser needed)
# ---------------------------------------------------------------------------


class TestScraperConstants:
    """Tests for scraper module constants and error classes."""

    def setup_method(self) -> None:
        self.scraper_mod = _load_module(
            "sundhed_dk_scraper", _SUNDHED_DIR / "scraper.py"
        )

    def test_section_urls_defined(self) -> None:
        assert "lab_results" in self.scraper_mod.SECTION_URLS
        assert "medications" in self.scraper_mod.SECTION_URLS
        assert "vaccinations" in self.scraper_mod.SECTION_URLS
        assert "ejournal" in self.scraper_mod.SECTION_URLS
        assert "appointments" in self.scraper_mod.SECTION_URLS
        assert len(self.scraper_mod.SECTION_URLS) == 5

    def test_session_expired_error_exists(self) -> None:
        assert issubclass(self.scraper_mod.SessionExpiredError, Exception)

    def test_all_urls_use_https(self) -> None:
        for url in self.scraper_mod.SECTION_URLS.values():
            assert url.startswith("https://")

    def test_urls_use_min_sundhedsjournal(self) -> None:
        """Verify URLs use the correct sundhed.dk path structure."""
        for name, url in self.scraper_mod.SECTION_URLS.items():
            if name != "ejournal":
                assert (
                    "min-sundhedsjournal" in url
                ), f"{name} URL missing min-sundhedsjournal"


# ---------------------------------------------------------------------------
# Browser unit tests (no actual browser launched)
# ---------------------------------------------------------------------------


class TestBrowserConstants:
    """Tests for browser module constants."""

    def setup_method(self) -> None:
        self.browser_mod = _load_module(
            "sundhed_dk_browser", _SUNDHED_DIR / "browser.py"
        )

    def test_login_url_uses_https(self) -> None:
        assert self.browser_mod.LOGIN_URL.startswith("https://")

    def test_browser_data_dir_uses_home(self) -> None:
        data_dir = str(self.browser_mod.BROWSER_DATA_DIR)
        assert data_dir.startswith(str(Path.home()))
        assert "sundhed_dk_browser" in data_dir

    def test_auth_timeout_reasonable(self) -> None:
        assert self.browser_mod.AUTH_TIMEOUT_S >= 60
        assert self.browser_mod.AUTH_TIMEOUT_S <= 300

    def test_auth_timeout_error_exists(self) -> None:
        assert issubclass(self.browser_mod.AuthTimeoutError, Exception)
