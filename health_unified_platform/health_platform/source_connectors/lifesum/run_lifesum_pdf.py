"""Lifesum PDF download orchestrator.

Queries silver.daily_meal for the latest data date, then downloads
a 7-day PDF covering the period from that date to today.

Usage:
    python -m health_platform.source_connectors.lifesum.run_lifesum_pdf
"""

from __future__ import annotations

import sys
from datetime import date

import duckdb
from health_platform.source_connectors.lifesum.browser import LifesumBrowser
from health_platform.source_connectors.lifesum.downloader import download_weekly_pdf
from health_platform.source_connectors.lifesum.pdf_parser import pdf_to_parquet
from health_platform.utils.logging_config import get_logger
from health_platform.utils.paths import get_db_path

logger = get_logger("lifesum.run_pdf")


def get_latest_meal_date() -> date | None:
    """Query silver.daily_meal for the most recent date."""
    db_path = get_db_path()
    if not db_path.exists():
        logger.warning("Database not found: %s", db_path)
        return None

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        result = con.execute("SELECT MAX(date) FROM silver.daily_meal").fetchone()
        if result and result[0]:
            return result[0]
        return None
    except Exception as exc:
        logger.warning("Could not query silver.daily_meal: %s", exc)
        return None
    finally:
        con.close()


def main() -> int:
    """Launch browser, authenticate, download PDF, parse to parquet, close."""
    latest = get_latest_meal_date()
    if latest:
        logger.info("Latest meal data in silver: %s", latest)
    else:
        logger.info("No existing meal data found — downloading for today")

    browser = LifesumBrowser()
    try:
        page = browser.launch_and_authenticate()

        # Step 1: Download PDF
        pdf_path = download_weekly_pdf(page, end_date=date.today())
        if not pdf_path:
            logger.error("PDF download failed")
            return 1

        # Step 2: Parse PDF to parquet (lands in food parquet dir for bronze pickup)
        parquet_path = pdf_to_parquet(pdf_path)
        if parquet_path:
            logger.info("Pipeline ready: %s → %s", pdf_path.name, parquet_path.name)
            return 0
        else:
            logger.error("PDF parsing failed")
            return 1

    except Exception as exc:
        logger.error("Lifesum PDF pipeline error: %s", exc)
        return 1
    finally:
        browser.close()


if __name__ == "__main__":
    sys.exit(main())
