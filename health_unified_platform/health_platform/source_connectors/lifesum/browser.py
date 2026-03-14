"""Playwright browser lifecycle and Lifesum authentication.

Launches Chromium with persistent context (cookies reused ~30 days).
If cookies have expired, attempts automated email/password login
using credentials from the macOS keychain. Falls back to manual
login if automated login fails (e.g. CAPTCHA, changed form).
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

from health_platform.utils.logging_config import get_logger
from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright

logger = get_logger("lifesum.browser")

# Persistent browser data directory for session reuse
BROWSER_DATA_DIR = Path.home() / ".config" / "health_reporting" / "lifesum_browser"

# Lifesum URLs
LOGIN_URL = "https://lifesum.com/da/"

# Keychain service name for credentials
KEYCHAIN_SERVICE = "Lifesum login"
KEYCHAIN_PATH = Path.home() / "Library" / "Keychains" / "claude.keychain-db"

# URL fragments that indicate a logged-in state
LOGGED_IN_URL_FRAGMENTS = ["/account", "/profile", "/dashboard", "/diary", "/home"]

# URL fragments that indicate still on login/auth pages
LOGIN_URL_FRAGMENTS = ["/login", "/signup", "/register", "/auth", "/oauth"]

# Auth detection
AUTH_POLL_INTERVAL_S = 2.0
AUTH_TIMEOUT_S = 180.0

# Selectors that indicate a logged-in state
AUTH_LOGGED_IN_SELECTORS = [
    "button:has-text('Log af')",
    "a:has-text('Min konto')",
    "a[href*='/account']",
    ".user-avatar",
    "[data-testid='user-menu']",
    ".dashboard-content",
]


class AuthTimeoutError(Exception):
    """Raised when Lifesum authentication times out."""


def _get_credentials() -> tuple[str, str] | None:
    """Read Lifesum email and password from macOS keychain.

    Uses two separate `security` calls for reliability:
    - `-w` to get just the password
    - `-g` to parse the account (email) from attributes
    """
    try:
        # Get password via -w (outputs just the password to stdout)
        pw_result = subprocess.run(
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
        if pw_result.returncode != 0:
            logger.warning(
                "Keychain password lookup failed (rc=%d)", pw_result.returncode
            )
            return None
        password = pw_result.stdout.strip()

        # Get account (email) — no -g flag avoids permission prompt
        attr_result = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-s",
                KEYCHAIN_SERVICE,
                str(KEYCHAIN_PATH),
            ],
            capture_output=True,
            text=True,
        )
        account = None
        all_output = attr_result.stdout + "\n" + attr_result.stderr
        for line in all_output.splitlines():
            if '"acct"' in line and "=" in line:
                account = line.split("=", 1)[1].strip().strip('"')
                break

        if account and password:
            logger.info("Credentials loaded from keychain")
            return (account, password)

        logger.warning(
            "Credentials incomplete (account=%s, pw=%s)", bool(account), bool(password)
        )
        return None
    except Exception as exc:
        logger.warning("Keychain access failed: %s", exc)
        return None


class LifesumBrowser:
    """Manages Playwright browser lifecycle for Lifesum scraping.

    Authentication strategy (in order):
    1. Persistent cookies (from previous session)
    2. Automated email/password login (credentials from keychain)
    3. Manual login (user logs in via visible browser window)
    """

    def __init__(self, headless: bool = False) -> None:
        self._headless = headless
        self._playwright = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None

    def launch_and_authenticate(self) -> Page:
        """Launch browser, navigate to Lifesum, and authenticate.

        Returns the authenticated Page object ready for scraping.
        """
        BROWSER_DATA_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
        BROWSER_DATA_DIR.chmod(0o700)

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch_persistent_context(
            user_data_dir=str(BROWSER_DATA_DIR),
            headless=self._headless,
            locale="da-DK",
            viewport={"width": 1280, "height": 900},
        )
        self._context = self._browser
        self._page = self._context.new_page()

        logger.info("Navigating to %s", LOGIN_URL)
        self._page.goto(LOGIN_URL, wait_until="domcontentloaded")

        # Wait for SPA to render
        self._page.wait_for_timeout(3000)

        # Strategy 1: Already logged in via cookies
        if self._is_logged_in():
            logger.info("Already logged in (session reuse)")
            return self._page

        # Strategy 2: Automated login with keychain credentials
        logger.info("Not logged in — attempting automated login...")
        if self._try_automated_login():
            logger.info("Automated login successful")
            return self._page

        # Strategy 3: Manual login fallback
        logger.info(
            "Automated login failed — waiting for manual login (timeout: %ds)...",
            AUTH_TIMEOUT_S,
        )
        print(
            "\n========================================\n"
            "  Automated login failed.\n"
            "  Please log in manually in the browser.\n"
            "  Waiting up to 3 minutes...\n"
            "========================================"
        )
        self._wait_for_auth()
        logger.info("Authentication successful")
        return self._page

    def _try_automated_login(self) -> bool:
        """Attempt to log in using email/password from keychain."""
        if self._page is None:
            return False

        credentials = _get_credentials()
        if not credentials:
            logger.info("No credentials in keychain — skipping automated login")
            return False

        email, password = credentials

        try:
            # Click "Log på" button to open login form
            login_btn = self._page.query_selector("a:has-text('Log på')")
            if not login_btn:
                login_btn = self._page.query_selector("a:has-text('Log in')")
            if not login_btn:
                login_btn = self._page.query_selector("[href*='login']")

            if login_btn:
                logger.info("Clicking login button...")
                login_btn.click()
                self._page.wait_for_timeout(2000)

            # Look for email input field
            email_input = (
                self._page.query_selector("input[type='email']")
                or self._page.query_selector("input[name='email']")
                or self._page.query_selector("input[placeholder*='mail']")
                or self._page.query_selector("input[placeholder*='Mail']")
            )

            if not email_input:
                # Try "Continue with email" button first
                email_btn = (
                    self._page.query_selector("button:has-text('mail')")
                    or self._page.query_selector("a:has-text('mail')")
                    or self._page.query_selector(
                        "button:has-text('Fortsæt med mailadresse')"
                    )
                )
                if email_btn:
                    logger.info("Clicking 'Continue with email' button...")
                    email_btn.click()
                    self._page.wait_for_timeout(2000)
                    email_input = self._page.query_selector(
                        "input[type='email']"
                    ) or self._page.query_selector("input[name='email']")

            if not email_input:
                logger.warning("Could not find email input field")
                return False

            # Fill email
            logger.info("Filling email field...")
            email_input.fill(email)

            # Look for password field (might appear after email submission)
            password_input = self._page.query_selector(
                "input[type='password']"
            ) or self._page.query_selector("input[name='password']")

            if not password_input:
                # Submit email first, then look for password
                submit_btn = (
                    self._page.query_selector("button[type='submit']")
                    or self._page.query_selector("button:has-text('Næste')")
                    or self._page.query_selector("button:has-text('Next')")
                    or self._page.query_selector("button:has-text('Fortsæt')")
                    or self._page.query_selector("button:has-text('Continue')")
                )
                if submit_btn:
                    submit_btn.click()
                    self._page.wait_for_timeout(2000)
                password_input = self._page.query_selector(
                    "input[type='password']"
                ) or self._page.query_selector("input[name='password']")

            if not password_input:
                logger.warning("Could not find password input field")
                return False

            # Fill password and submit
            logger.info("Filling password field...")
            password_input.fill(password)

            submit_btn = (
                self._page.query_selector("button[type='submit']")
                or self._page.query_selector("button:has-text('Log')")
                or self._page.query_selector("button:has-text('Sign in')")
            )
            if submit_btn:
                submit_btn.click()
            else:
                password_input.press("Enter")

            # Wait for redirect after login
            logger.info("Waiting for login redirect...")
            self._page.wait_for_timeout(5000)

            return self._is_logged_in()

        except Exception as exc:
            logger.warning("Automated login failed: %s", exc)
            return False

    def _is_logged_in(self) -> bool:
        """Check if the current page indicates a logged-in state."""
        if self._page is None:
            return False

        current_url = self._page.url.lower()

        # Negative: still on a login/auth page
        if any(frag in current_url for frag in LOGIN_URL_FRAGMENTS):
            return False

        # Positive: URL contains a known logged-in fragment
        if any(frag in current_url for frag in LOGGED_IN_URL_FRAGMENTS):
            logger.debug("Login detected via URL fragment: %s", current_url)
            return True

        # Positive: any known logged-in DOM element
        for selector in AUTH_LOGGED_IN_SELECTORS:
            try:
                if self._page.query_selector(selector):
                    logger.debug("Login detected via selector: %s", selector)
                    return True
            except Exception:
                continue

        return False

    def _wait_for_auth(self) -> None:
        """Poll for logged-in state. Falls back to manual Enter prompt or signal file."""
        signal_file = BROWSER_DATA_DIR / ".auth_ready"
        start = time.monotonic()
        last_log = 0.0

        while time.monotonic() - start < AUTH_TIMEOUT_S:
            if self._is_logged_in():
                return

            # Also check for a signal file (useful when stdin is not available)
            if signal_file.exists():
                signal_file.unlink()
                logger.info("Auth signal file detected")
                time.sleep(1.0)
                if self._page:
                    self._page.wait_for_load_state("domcontentloaded")
                return

            # Log current URL every 30s for debugging
            elapsed = time.monotonic() - start
            if elapsed - last_log >= 30.0 and self._page:
                logger.info("Still waiting... (%.0fs) URL: %s", elapsed, self._page.url)
                last_log = elapsed

            time.sleep(AUTH_POLL_INTERVAL_S)

        # Fallback: manual confirmation (only works with interactive terminal)
        logger.warning("Auto-detection timed out. Falling back to manual confirmation.")
        print("\nCould not auto-detect login state.")
        try:
            input("Press Enter when you are logged in to Lifesum... ")
        except EOFError:
            raise AuthTimeoutError(
                f"Authentication not detected within {AUTH_TIMEOUT_S}s "
                "and stdin not available for manual confirmation. "
                "Run this script in an interactive terminal, or create "
                f"'{signal_file}' after logging in."
            )

        # Give page a moment to settle after user confirms
        time.sleep(1.0)
        if self._page:
            self._page.wait_for_load_state("domcontentloaded")

    @property
    def page(self) -> Page | None:
        return self._page

    def close(self) -> None:
        """Close browser and clean up Playwright resources."""
        try:
            if self._context:
                self._context.close()
        except Exception as exc:
            logger.warning("Error closing browser context: %s", exc)

        try:
            if self._playwright:
                self._playwright.stop()
        except Exception as exc:
            logger.warning("Error stopping Playwright: %s", exc)

        self._page = None
        self._context = None
        self._browser = None
        self._playwright = None

        # Lock down browser data files (cookies, session storage)
        if BROWSER_DATA_DIR.exists():
            for root, _dirs, files in os.walk(BROWSER_DATA_DIR):
                for f in files:
                    try:
                        os.chmod(os.path.join(root, f), 0o600)
                    except OSError:
                        pass

        logger.info("Browser closed")
