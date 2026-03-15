"""Tests for Lifesum API download pipeline (JWT token + direct API).

Tests the api_downloader, cookie_extractor diagnostics, and
run_lifesum_pdf orchestrator with mocked tokens and HTTP responses.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from health_platform.source_connectors.lifesum.api_downloader import (
    AuthError,
    _align_to_monday,
    _build_session,
    _is_token_expired,
    download_pdf_via_api,
)
from health_platform.source_connectors.lifesum.cookie_extractor import (
    ANALYTICS_COOKIES,
    get_cookie_diagnostics,
)

# --- cookie_extractor tests ---


class TestCookieExtractor:
    """Tests for Chrome cookie diagnostics."""

    def test_analytics_cookies_frozenset(self):
        """ANALYTICS_COOKIES contains known tracking cookie names."""
        assert "_ga" in ANALYTICS_COOKIES
        assert "_fbp" in ANALYTICS_COOKIES
        assert "CookieConsent" in ANALYTICS_COOKIES
        assert "session_token" not in ANALYTICS_COOKIES

    @patch(
        "health_platform.source_connectors.lifesum.cookie_extractor.CHROME_COOKIES_PATH",
        Path("/nonexistent/path"),
    )
    def test_diagnostics_missing_db(self):
        """get_cookie_diagnostics returns error when DB not found."""
        result = get_cookie_diagnostics()
        assert "error" in result
        assert "not found" in result["error"]


# --- api_downloader tests ---


class TestAlignToMonday:
    """Tests for week alignment logic."""

    def test_monday_stays_monday(self):
        assert _align_to_monday(date(2026, 3, 9)) == date(2026, 3, 9)  # Monday

    def test_wednesday_aligns_to_monday(self):
        assert _align_to_monday(date(2026, 3, 11)) == date(2026, 3, 9)  # Wed -> Mon

    def test_sunday_aligns_to_monday(self):
        assert _align_to_monday(date(2026, 3, 15)) == date(2026, 3, 9)  # Sun -> Mon

    def test_saturday_aligns_to_monday(self):
        assert _align_to_monday(date(2026, 3, 14)) == date(2026, 3, 9)  # Sat -> Mon


class TestTokenExpiry:
    """Tests for JWT expiry checking."""

    def test_expired_token(self):
        """Token with past expiry is detected as expired."""
        # Build a test token with exp=1000000000 (2001-09-09)
        import base64

        header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
        payload = base64.urlsafe_b64encode(b'{"exp":1000000000}').rstrip(b"=").decode()
        token = f"{header}.{payload}.sig"
        assert _is_token_expired(token) is True

    def test_valid_token(self):
        """Token with far-future expiry is detected as valid."""
        import base64

        header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
        payload = base64.urlsafe_b64encode(b'{"exp":9999999999}').rstrip(b"=").decode()
        token = f"{header}.{payload}.sig"
        assert _is_token_expired(token) is False

    def test_malformed_token(self):
        """Malformed token is treated as expired (safe default)."""
        assert _is_token_expired("not-a-jwt") is True
        assert _is_token_expired("") is True


class TestBuildSession:
    """Tests for session construction with JWT token."""

    def test_session_has_bearer_auth(self):
        """_build_session sets Authorization: Bearer header."""
        session = _build_session("test-token-123")
        assert session.headers["Authorization"] == "Bearer test-token-123"

    def test_session_accepts_pdf(self):
        """_build_session requests PDF content type."""
        session = _build_session("token")
        assert session.headers["Accept"] == "application/pdf"

    def test_session_has_user_agent(self):
        """_build_session sets a Chrome-like User-Agent."""
        session = _build_session("token")
        ua = session.headers.get("User-Agent", "")
        assert "Chrome" in ua

    def test_session_has_origin(self):
        """_build_session sets lifesum.com as origin."""
        session = _build_session("token")
        assert "lifesum.com" in session.headers.get("Origin", "")


class TestDownloadPdfViaApi:
    """Tests for the JWT-based API download pipeline."""

    @patch(
        "health_platform.source_connectors.lifesum.api_downloader._get_token_from_keychain"
    )
    def test_no_token_raises_auth_error(self, mock_token):
        """Raises AuthError when no token in Keychain."""
        mock_token.return_value = None
        with pytest.raises(AuthError, match="No LIFESUM_API_TOKEN"):
            download_pdf_via_api(start_date=date(2026, 3, 1), end_date=date(2026, 3, 7))

    @patch("health_platform.source_connectors.lifesum.api_downloader._is_token_expired")
    @patch(
        "health_platform.source_connectors.lifesum.api_downloader._get_token_from_keychain"
    )
    def test_expired_token_raises_auth_error(self, mock_token, mock_expired):
        """Raises AuthError when token is expired."""
        mock_token.return_value = "expired-token"
        mock_expired.return_value = True
        with pytest.raises(AuthError, match="expired"):
            download_pdf_via_api(start_date=date(2026, 3, 1), end_date=date(2026, 3, 7))

    @patch("health_platform.source_connectors.lifesum.api_downloader._is_token_expired")
    @patch(
        "health_platform.source_connectors.lifesum.api_downloader._get_token_from_keychain"
    )
    def test_401_raises_auth_error(self, mock_token, mock_expired, tmp_path):
        """Raises AuthError on 401 response from API."""
        mock_token.return_value = "bad-token"
        mock_expired.return_value = False

        mock_resp = MagicMock()
        mock_resp.status_code = 401

        pdf_dir = tmp_path / "pdf_archive"
        pdf_dir.mkdir()

        with patch(
            "health_platform.source_connectors.lifesum.api_downloader.PDF_ARCHIVE_DIR",
            pdf_dir,
        ):
            with patch("requests.Session.get", return_value=mock_resp):
                with pytest.raises(AuthError, match="401"):
                    download_pdf_via_api(
                        start_date=date(2026, 3, 9), end_date=date(2026, 3, 9)
                    )

    def test_existing_pdf_skips_download(self, tmp_path):
        """Skips download when PDF already exists."""
        pdf_dir = tmp_path / "pdf_archive"
        pdf_dir.mkdir()
        # Monday 2026-03-09 to Sunday 2026-03-15
        pdf_file = pdf_dir / "lifesum_nutrition_2026-03-09_to_2026-03-15.pdf"
        pdf_file.write_text("fake pdf")

        with patch(
            "health_platform.source_connectors.lifesum.api_downloader.PDF_ARCHIVE_DIR",
            pdf_dir,
        ):
            with patch(
                "health_platform.source_connectors.lifesum.api_downloader._get_token_from_keychain",
                return_value="valid-token",
            ):
                with patch(
                    "health_platform.source_connectors.lifesum.api_downloader._is_token_expired",
                    return_value=False,
                ):
                    result = download_pdf_via_api(
                        start_date=date(2026, 3, 9), end_date=date(2026, 3, 15)
                    )
        assert result == pdf_file

    @patch("health_platform.source_connectors.lifesum.api_downloader._is_token_expired")
    @patch(
        "health_platform.source_connectors.lifesum.api_downloader._get_token_from_keychain"
    )
    def test_successful_download(self, mock_token, mock_expired, tmp_path):
        """Downloads and saves PDF when API returns 200."""
        mock_token.return_value = "valid-token"
        mock_expired.return_value = False

        pdf_dir = tmp_path / "pdf_archive"
        pdf_dir.mkdir()

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "application/pdf"}
        mock_resp.content = b"%PDF-1.4 fake pdf content"
        mock_resp.raise_for_status = MagicMock()

        with patch(
            "health_platform.source_connectors.lifesum.api_downloader.PDF_ARCHIVE_DIR",
            pdf_dir,
        ):
            with patch("requests.Session.get", return_value=mock_resp):
                result = download_pdf_via_api(
                    start_date=date(2026, 3, 9), end_date=date(2026, 3, 9)
                )

        assert result is not None
        assert result.exists()
        assert result.stat().st_size > 0


# --- run_lifesum_pdf orchestrator tests ---


class TestRunLifesumPdfOrchestration:
    """Tests for the API-first, browser-fallback strategy in main()."""

    @patch("health_platform.source_connectors.lifesum.run_lifesum_pdf.pdf_to_parquet")
    @patch(
        "health_platform.source_connectors.lifesum.run_lifesum_pdf.download_pdf_via_api"
    )
    @patch(
        "health_platform.source_connectors.lifesum.run_lifesum_pdf.get_latest_meal_date"
    )
    @patch(
        "health_platform.source_connectors.lifesum.run_lifesum_pdf.check_session_freshness"
    )
    def test_api_success_skips_browser(
        self, mock_fresh, mock_latest, mock_api, mock_parse
    ):
        """When API download succeeds, browser is never launched."""
        mock_fresh.return_value = True
        mock_latest.return_value = date(2026, 3, 10)
        mock_api.return_value = Path("/fake/pdf.pdf")
        mock_parse.return_value = Path("/fake/parquet.parquet")

        from health_platform.source_connectors.lifesum.run_lifesum_pdf import main

        with patch("sys.argv", ["prog"]):
            result = main()

        assert result == 0
        mock_api.assert_called_once()

    @patch("health_platform.source_connectors.lifesum.run_lifesum_pdf.pdf_to_parquet")
    @patch(
        "health_platform.source_connectors.lifesum.run_lifesum_pdf.download_weekly_pdf"
    )
    @patch("health_platform.source_connectors.lifesum.run_lifesum_pdf.LifesumBrowser")
    @patch(
        "health_platform.source_connectors.lifesum.run_lifesum_pdf.download_pdf_via_api"
    )
    @patch(
        "health_platform.source_connectors.lifesum.run_lifesum_pdf.get_latest_meal_date"
    )
    @patch(
        "health_platform.source_connectors.lifesum.run_lifesum_pdf.check_session_freshness"
    )
    def test_api_failure_falls_back_to_browser(
        self, mock_fresh, mock_latest, mock_api, mock_browser_cls, mock_dl, mock_parse
    ):
        """When API raises AuthError, falls back to Playwright browser."""
        mock_fresh.return_value = True
        mock_latest.return_value = date(2026, 3, 10)
        mock_api.side_effect = AuthError("no token")
        mock_browser = MagicMock()
        mock_browser_cls.return_value = mock_browser
        mock_browser.launch_and_authenticate.return_value = MagicMock()
        mock_dl.return_value = Path("/fake/pdf.pdf")
        mock_parse.return_value = Path("/fake/parquet.parquet")

        from health_platform.source_connectors.lifesum.run_lifesum_pdf import main

        with patch("sys.argv", ["prog"]):
            result = main()

        assert result == 0
        mock_api.assert_called_once()
        mock_browser_cls.assert_called_once()
        mock_dl.assert_called_once()
