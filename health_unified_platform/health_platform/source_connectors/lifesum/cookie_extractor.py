"""Extract Chrome cookies for Lifesum on macOS.

Uses pycookiecheat for reliable decryption across Chrome versions.
Falls back to raw SQLite metadata if decryption fails.

Requirements:
    - pycookiecheat package
    - macOS only (uses Keychain + Chrome Safe Storage)
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
from pathlib import Path

from health_platform.utils.logging_config import get_logger

logger = get_logger("lifesum.cookie_extractor")

CHROME_COOKIES_PATH = (
    Path.home()
    / "Library"
    / "Application Support"
    / "Google"
    / "Chrome"
    / "Default"
    / "Cookies"
)

LIFESUM_URL = "https://lifesum.com"

# Domains to check in diagnostics
LIFESUM_DOMAINS = [
    ".lifesum.com",
    "lifesum.com",
    "www.lifesum.com",
    "api.lifesum.com",
    "app.lifesum.com",
]

# Known analytics-only cookies (not auth-relevant)
ANALYTICS_COOKIES = frozenset(
    {
        "_ga",
        "_ga_Z8G409CFY1",
        "_fbp",
        "_gcl_au",
        "CookieConsent",
    }
)


def extract_chrome_cookies(
    url: str = LIFESUM_URL,
) -> dict[str, str]:
    """Extract and decrypt Chrome cookies for a URL using pycookiecheat.

    Args:
        url: URL to extract cookies for. Defaults to lifesum.com.

    Returns:
        Dict of {cookie_name: decrypted_value} for all matching cookies.
        Empty dict if no cookies found or extraction fails.
    """
    try:
        from pycookiecheat import chrome_cookies
    except ImportError:
        logger.error("pycookiecheat not installed. Run: pip install pycookiecheat")
        return {}

    try:
        cookies = chrome_cookies(url)
        logger.info("Extracted %d cookies for %s", len(cookies), url)
        for name, value in cookies.items():
            display = value[:30] + "..." if len(value) > 30 else value
            logger.debug("  %s = %s", name, display)
        return cookies
    except Exception as exc:
        logger.warning("Chrome cookie extraction failed: %s", exc)
        return {}


def get_cookie_diagnostics() -> dict:
    """Return diagnostic info about Lifesum cookies in Chrome.

    Useful for debugging auth issues. Returns raw metadata
    without decrypting values (safe to log).
    """
    if not CHROME_COOKIES_PATH.exists():
        return {
            "error": "Chrome Cookies DB not found",
            "path": str(CHROME_COOKIES_PATH),
        }

    fd, tmp_str = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    tmp_path = Path(tmp_str)
    try:
        shutil.copy2(str(CHROME_COOKIES_PATH), str(tmp_path))
    except OSError as exc:
        tmp_path.unlink(missing_ok=True)
        return {"error": f"Could not copy Cookies DB: {exc}"}

    diagnostics: dict = {"chrome_cookies_path": str(CHROME_COOKIES_PATH), "cookies": []}

    try:
        con = sqlite3.connect(f"file:{tmp_path}?mode=ro", uri=True)
        domain_clauses = " OR ".join(["host_key = ?" for _ in LIFESUM_DOMAINS])
        query = f"""
            SELECT host_key, name, length(encrypted_value) as enc_len, path,
                   datetime(expires_utc / 1000000 - 11644473600, 'unixepoch') as expires,
                   is_httponly, is_secure, is_persistent
            FROM cookies
            WHERE {domain_clauses}
            ORDER BY host_key, name
        """
        rows = con.execute(query, LIFESUM_DOMAINS).fetchall()

        for row in rows:
            diagnostics["cookies"].append(
                {
                    "domain": row[0],
                    "name": row[1],
                    "encrypted_size": row[2],
                    "path": row[3],
                    "expires": row[4],
                    "httponly": bool(row[5]),
                    "secure": bool(row[6]),
                    "persistent": bool(row[7]),
                }
            )

        con.close()
    except sqlite3.OperationalError as exc:
        diagnostics["error"] = str(exc)
    finally:
        tmp_path.unlink(missing_ok=True)

    diagnostics["total_cookies"] = len(diagnostics["cookies"])
    diagnostics["auth_cookies"] = [
        c for c in diagnostics["cookies"] if c["name"] not in ANALYTICS_COOKIES
    ]

    return diagnostics
