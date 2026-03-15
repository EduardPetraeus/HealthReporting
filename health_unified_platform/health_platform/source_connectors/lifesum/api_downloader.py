"""Download Lifesum nutrition PDF via direct API (no browser needed).

Uses a JWT Bearer token (stored in macOS Keychain) to call the
Lifesum food-tracker export API directly.

API endpoint (discovered 2026-03-15):
    GET https://api.lifesum.com/food-tracker/v1/export/week/{YYYY-MM-DD}
    Authorization: Bearer <JWT>
    Accept: application/pdf
    Returns: application/pdf (7-day export, Monday to Sunday)

Token lifecycle:
    - JWT valid for 24 hours (exp - iat = 86400s)
    - Obtained after Apple Sign-In via Lifesum web app
    - Stored in Keychain as LIFESUM_API_TOKEN
    - When expired: Playwright browser re-auth captures a new token
"""

from __future__ import annotations

import json
import subprocess
import time
from datetime import date, timedelta
from pathlib import Path

import requests
from health_platform.utils.logging_config import get_logger
from health_platform.utils.paths import get_data_lake_root

logger = get_logger("lifesum.api_downloader")

PDF_ARCHIVE_DIR = get_data_lake_root() / "lifesum" / "pdf_archive"
API_BASE = "https://api.lifesum.com/food-tracker/v1"
EXPORT_ENDPOINT = f"{API_BASE}/export/week"

KEYCHAIN_SERVICE = "LIFESUM_API_TOKEN"
KEYCHAIN_PATH = Path.home() / "Library" / "Keychains" / "claude.keychain-db"


class AuthError(Exception):
    """Raised when Lifesum API authentication fails."""


def _get_token_from_keychain() -> str | None:
    """Read the Lifesum JWT from macOS Keychain."""
    result = subprocess.run(
        [
            "security",
            "find-generic-password",
            "-s",
            KEYCHAIN_SERVICE,
            "-w",
            str(KEYCHAIN_PATH),
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.warning("No LIFESUM_API_TOKEN in Keychain")
        return None
    return result.stdout.strip()


def _save_token_to_keychain(token: str) -> None:
    """Save or update the Lifesum JWT in macOS Keychain."""
    # Delete existing entry first (ignore errors if not found)
    subprocess.run(
        [
            "security",
            "delete-generic-password",
            "-s",
            KEYCHAIN_SERVICE,
            str(KEYCHAIN_PATH),
        ],
        capture_output=True,
    )
    subprocess.run(
        [
            "security",
            "add-generic-password",
            "-s",
            KEYCHAIN_SERVICE,
            "-a",
            "lifesum",
            "-w",
            token,
            str(KEYCHAIN_PATH),
        ],
        capture_output=True,
        check=True,
    )
    logger.info("Saved new LIFESUM_API_TOKEN to Keychain")


def _is_token_expired(token: str) -> bool:
    """Check if a JWT is expired by decoding the payload (no verification)."""
    try:
        import base64

        # JWT = header.payload.signature — decode payload (part 2)
        payload_b64 = token.split(".")[1]
        # Add padding if needed
        padding = 4 - len(payload_b64) % 4
        if padding != 4:
            payload_b64 += "=" * padding
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        exp = payload.get("exp", 0)
        now = time.time()
        if now >= exp:
            logger.info(
                "Token expired (exp=%d, now=%d, delta=%.0fs)", exp, int(now), now - exp
            )
            return True
        remaining = exp - now
        logger.info("Token valid (%.0f hours remaining)", remaining / 3600)
        return False
    except Exception as exc:
        logger.warning("Could not decode JWT to check expiry: %s", exc)
        return True  # Assume expired if we can't check


def _build_session(token: str) -> requests.Session:
    """Create a requests session with JWT auth and browser-like headers."""
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/pdf",
            "Content-Type": "application/json",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/146.0.0.0 Safari/537.36"
            ),
            "Origin": "https://lifesum.com",
            "Referer": "https://lifesum.com/",
        }
    )
    return session


def _align_to_monday(d: date) -> date:
    """Align a date to the Monday of its week (Lifesum exports are Mon-Sun)."""
    return d - timedelta(days=d.weekday())


def download_pdf_via_api(
    start_date: date | None = None,
    end_date: date | None = None,
) -> Path | None:
    """Download Lifesum nutrition PDFs using the direct API.

    Downloads one PDF per week (Monday-Sunday) covering the requested range.
    Each API call returns a 7-day PDF starting from the given Monday.

    Args:
        start_date: First day of the export range.
        end_date: Last day of the export range. Defaults to today.

    Returns:
        Path to the last saved PDF, or None if download failed.
        Raises AuthError if token is missing or expired.
    """
    end_date = end_date or date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=6)

    PDF_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Get token from Keychain
    token = _get_token_from_keychain()
    if not token:
        raise AuthError(
            "No LIFESUM_API_TOKEN in Keychain. "
            "Log into lifesum.com, download a PDF via Chrome, "
            "then capture the Bearer token from DevTools."
        )

    if _is_token_expired(token):
        raise AuthError(
            "LIFESUM_API_TOKEN is expired (24h lifetime). "
            "Log into lifesum.com in Chrome to get a new token."
        )

    session = _build_session(token)

    # Step 2: Download PDFs for each week in the range
    monday = _align_to_monday(start_date)
    end_monday = _align_to_monday(end_date)
    last_saved = None

    while monday <= end_monday:
        sunday = monday + timedelta(days=6)
        filename = f"lifesum_nutrition_{monday.isoformat()}_to_{sunday.isoformat()}.pdf"
        output_path = PDF_ARCHIVE_DIR / filename

        if output_path.exists() and output_path.stat().st_size > 0:
            logger.info("PDF already exists: %s, skipping", filename)
            last_saved = output_path
            monday += timedelta(days=7)
            continue

        url = f"{EXPORT_ENDPOINT}/{monday.isoformat()}"
        logger.info("Downloading: %s", url)

        try:
            resp = session.get(url, timeout=30)

            if resp.status_code == 401:
                raise AuthError("Token rejected (401). Re-authenticate in Chrome.")
            if resp.status_code == 403:
                raise AuthError("Access denied (403). Token may be for wrong user.")

            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "")
            if "pdf" not in content_type.lower():
                logger.warning("Unexpected content type: %s", content_type)

            with open(output_path, "wb") as f:
                f.write(resp.content)

            if output_path.stat().st_size > 0:
                size_kb = output_path.stat().st_size / 1024
                logger.info("Saved: %s (%.1f KB)", filename, size_kb)
                last_saved = output_path
            else:
                logger.warning("Empty response for %s", monday.isoformat())
                output_path.unlink()

        except AuthError:
            raise
        except requests.RequestException as exc:
            logger.error("Download failed for %s: %s", monday.isoformat(), exc)
            if output_path.exists():
                output_path.unlink()

        monday += timedelta(days=7)

    return last_saved


def diagnose() -> dict:
    """Run diagnostics on API download readiness."""
    report: dict = {
        "token_present": False,
        "token_expired": True,
        "api_reachable": False,
        "test_download": None,
    }

    token = _get_token_from_keychain()
    if not token:
        report["summary"] = "NO_TOKEN — store JWT in Keychain as LIFESUM_API_TOKEN"
        return report

    report["token_present"] = True
    report["token_expired"] = _is_token_expired(token)

    if report["token_expired"]:
        report["summary"] = "TOKEN_EXPIRED — re-authenticate in Chrome"
        return report

    # Try a test request
    session = _build_session(token)
    try:
        monday = _align_to_monday(date.today())
        url = f"{EXPORT_ENDPOINT}/{monday.isoformat()}"
        resp = session.get(url, timeout=15)
        report["api_reachable"] = True
        report["test_status"] = resp.status_code
        report["test_content_type"] = resp.headers.get("Content-Type", "")
        report["test_size_kb"] = len(resp.content) / 1024

        if (
            resp.status_code == 200
            and "pdf" in resp.headers.get("Content-Type", "").lower()
        ):
            report["summary"] = "READY — API download works"
        elif resp.status_code == 401:
            report["summary"] = "TOKEN_REJECTED — re-authenticate in Chrome"
        else:
            report["summary"] = f"UNEXPECTED — status={resp.status_code}"
    except requests.RequestException as exc:
        report["summary"] = f"API_ERROR — {exc}"

    return report
