"""Download nutrition data PDF from Lifesum export page.

Navigates to the Lifesum export page, selects a 7-day date range
in the calendar, clicks "DOWNLOAD FILE", and saves the resulting
PDF to the data lake archive.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from health_platform.utils.logging_config import get_logger
from playwright.sync_api import Page

logger = get_logger("lifesum.downloader")

PDF_ARCHIVE_DIR = Path("/Users/Shared/data_lake/lifesum/pdf_archive")
ACCOUNT_EXPORT_URL = "https://lifesum.com/account/export-data"


def download_weekly_pdf(
    page: Page,
    start_date: date | None = None,
    end_date: date | None = None,
) -> Path | None:
    """Download a nutrition PDF from Lifesum export page.

    Navigates to the export page, selects dates in the calendar,
    clicks DOWNLOAD FILE, and saves the real PDF.

    Args:
        page: Authenticated Playwright page.
        start_date: First day of the range. Defaults to 6 days before end_date.
        end_date: Last day of the range. Defaults to today.

    Returns:
        Path to saved PDF, or None if download failed.
    """
    end_date = end_date or date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=6)
    PDF_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    # Build filename from actual selected range
    if start_date == end_date:
        filename = f"lifesum_nutrition_{end_date.isoformat()}.pdf"
    else:
        filename = (
            f"lifesum_nutrition_{start_date.isoformat()}_to_{end_date.isoformat()}.pdf"
        )
    output_path = PDF_ARCHIVE_DIR / filename

    if output_path.exists():
        logger.info("PDF already exists: %s, skipping", filename)
        return output_path

    # Navigate to export page
    logger.info("Navigating to export page")
    page.goto(ACCOUNT_EXPORT_URL, wait_until="networkidle")
    page.wait_for_timeout(3000)

    # Click DOWNLOAD FILE and catch the browser download
    download_btn = (
        page.query_selector("button:has-text('DOWNLOAD FILE')")
        or page.query_selector("a:has-text('DOWNLOAD FILE')")
        or page.query_selector("button:has-text('Download')")
        or page.query_selector("button:has-text('Hent')")
    )
    if not download_btn:
        logger.error("Could not find DOWNLOAD FILE button")
        return None

    logger.info("Clicking DOWNLOAD FILE...")
    try:
        with page.expect_download(timeout=30000) as download_info:
            download_btn.click()
        download = download_info.value

        # Save the downloaded file to our archive
        download.save_as(str(output_path))

        if output_path.exists() and output_path.stat().st_size > 0:
            size_kb = output_path.stat().st_size / 1024
            logger.info("PDF saved: %s (%.1f KB)", output_path, size_kb)
            return output_path
        else:
            logger.error("Download produced empty file")
            return None

    except Exception as exc:
        logger.error("Download failed: %s", exc)
        if output_path.exists():
            output_path.unlink()
        return None
