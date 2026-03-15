"""Lifesum PDF download orchestrator.

Queries silver.daily_meal for the latest data date, then downloads
a PDF covering the gap period. Supports --login-only for manual
session refresh (Apple ID requires Touch ID / 2FA).

Usage:
    python -m health_platform.source_connectors.lifesum.run_lifesum_pdf
    python -m health_platform.source_connectors.lifesum.run_lifesum_pdf --login-only
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date, datetime, timedelta

import duckdb
from health_platform.source_connectors.lifesum.browser import (
    BROWSER_DATA_DIR,
    LifesumBrowser,
)
from health_platform.source_connectors.lifesum.downloader import download_weekly_pdf
from health_platform.source_connectors.lifesum.pdf_parser import pdf_to_parquet
from health_platform.utils.logging_config import get_logger
from health_platform.utils.paths import get_db_path

logger = get_logger("lifesum.run_pdf")

# Session older than this triggers a warning
SESSION_MAX_AGE_DAYS = 25
NTFY_TOPIC = "health-reporting"


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


def get_session_age_days() -> float | None:
    """Return age of the browser session in days, or None if no session."""
    cookie_db = BROWSER_DATA_DIR / "Default" / "Cookies"
    if not cookie_db.exists():
        return None
    mtime = datetime.fromtimestamp(cookie_db.stat().st_mtime)
    return (datetime.now() - mtime).total_seconds() / 86400


def send_ntfy_reminder(days_until_expiry: int) -> None:
    """Send a push notification via ntfy.sh to remind about session refresh."""
    try:
        subprocess.run(
            [
                "curl",
                "-s",
                "-d",
                f"Lifesum session expires in ~{days_until_expiry} days. "
                f"Run: python -m health_platform.source_connectors.lifesum.run_lifesum_pdf --login-only",
                "-H",
                "Title: Lifesum Session Refresh Needed",
                "-H",
                "Priority: default",
                "-H",
                "Tags: cookie,warning",
                f"https://ntfy.sh/{NTFY_TOPIC}",
            ],
            capture_output=True,
            timeout=10,
        )
        logger.info("Sent ntfy.sh reminder (expires in ~%d days)", days_until_expiry)
    except Exception as exc:
        logger.warning("Failed to send ntfy reminder: %s", exc)


def check_session_freshness() -> bool:
    """Check session age and warn/notify if expiring soon. Returns True if OK."""
    age = get_session_age_days()
    if age is None:
        logger.warning("No browser session found — login required")
        return False
    days_remaining = 30 - age
    if age > SESSION_MAX_AGE_DAYS:
        logger.warning(
            "Browser session is %.0f days old (expires in ~%.0f days) — refresh recommended",
            age,
            days_remaining,
        )
        send_ntfy_reminder(int(days_remaining))
        return False
    logger.info(
        "Browser session is %.0f days old (OK, ~%.0f days remaining)",
        age,
        days_remaining,
    )
    return True


def login_only() -> int:
    """Open browser for manual login, then exit. Session persists ~30 days."""
    print(
        "\n========================================\n"
        "  Lifesum Manual Login\n"
        "  \n"
        "  A browser window will open.\n"
        "  Log in via Apple ID (Touch ID/2FA).\n"
        "  The session will be saved for ~30 days.\n"
        "========================================"
    )
    browser = LifesumBrowser(headless=False)
    try:
        browser.launch_and_authenticate()
        logger.info("Login successful — session saved to %s", BROWSER_DATA_DIR)
        print("\nLogin successful! Session saved. You can close this.")
        return 0
    except Exception as exc:
        logger.error("Login failed: %s", exc)
        return 1
    finally:
        browser.close()


def main() -> int:
    """Launch browser, authenticate, download PDF, parse to parquet, close."""
    parser = argparse.ArgumentParser(description="Lifesum PDF download pipeline")
    parser.add_argument(
        "--login-only",
        action="store_true",
        help="Only open browser for manual login (session refresh), then exit",
    )
    parser.add_argument(
        "--skip-freshness-check",
        action="store_true",
        help="Skip session freshness check",
    )
    args = parser.parse_args()

    if args.login_only:
        return login_only()

    # Check session freshness before attempting download
    if not args.skip_freshness_check:
        check_session_freshness()

    latest = get_latest_meal_date()
    today = date.today()

    if latest:
        start_date = latest + timedelta(days=1)
        logger.info(
            "Latest meal data in silver: %s — downloading from %s", latest, start_date
        )
        if start_date > today:
            logger.info("Already up to date (latest=%s, today=%s)", latest, today)
            return 0
    else:
        start_date = today - timedelta(days=6)
        logger.info(
            "No existing meal data — downloading last 7 days from %s", start_date
        )

    browser = LifesumBrowser()
    try:
        page = browser.launch_and_authenticate()

        # Step 1: Download PDF covering start_date to today
        pdf_path = download_weekly_pdf(page, start_date=start_date, end_date=today)
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
