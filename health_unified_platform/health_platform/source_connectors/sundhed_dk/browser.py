"""Playwright browser lifecycle and MitID authentication wait.

Launches a non-headless Chromium browser, navigates to sundhed.dk,
and waits for the user to complete MitID 2FA authentication.
Uses a persistent browser context to reuse sessions across runs.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from health_platform.utils.logging_config import get_logger

logger = get_logger("sundhed_dk.browser")

# Persistent browser data directory for session reuse
BROWSER_DATA_DIR = Path.home() / ".config" / "health_reporting" / "sundhed_dk_browser"

# sundhed.dk URLs
LOGIN_URL = "https://www.sundhed.dk/borger/min-side/"
LOGGED_IN_URL_FRAGMENT = "/borger/min-side"

# Auth detection
AUTH_POLL_INTERVAL_S = 2.0
AUTH_TIMEOUT_S = 180.0

# Selectors that indicate a logged-in state
AUTH_LOGGED_IN_SELECTORS = [
    "[data-logged-in='true']",
    ".user-profile",
    ".logged-in-user",
    "#user-menu",
    ".min-side-content",
]


class AuthTimeoutError(Exception):
    """Raised when MitID authentication times out."""


class SundhedDkBrowser:
    """Manages Playwright browser lifecycle for sundhed.dk scraping.

    Usage:
        browser = SundhedDkBrowser()
        page = browser.launch_and_authenticate()
        # ... scrape pages ...
        browser.close()
    """

    def __init__(self, headless: bool = False) -> None:
        self._headless = headless
        self._playwright = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None

    def launch_and_authenticate(self) -> Page:
        """Launch browser, navigate to sundhed.dk, and wait for MitID auth.

        Returns the authenticated Page object ready for scraping.
        """
        BROWSER_DATA_DIR.mkdir(parents=True, exist_ok=True)

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

        if self._is_logged_in():
            logger.info("Already logged in (session reuse)")
            return self._page

        logger.info(
            "Waiting for MitID authentication (timeout: %ds)...", AUTH_TIMEOUT_S
        )
        print(
            "\n========================================\n"
            "  Complete MitID login in the browser.\n"
            "  Waiting up to 2 minutes...\n"
            "========================================"
        )

        self._wait_for_auth()
        logger.info("Authentication successful")
        return self._page

    def _is_logged_in(self) -> bool:
        """Check if the current page indicates a logged-in state.

        sundhed.dk uses AngularJS. Key indicators:
        - NOT logged in: "Log på" button visible, icon-lock has ng-hide class
        - Logged in: login button has 'loggedin' class, or "Log ud" text present
        """
        if self._page is None:
            return False

        current_url = self._page.url.lower()

        # Negative: still on a login/auth page
        login_indicators = ["login", "mitid", "nemlog", "nemid", "oauth", "saml"]
        if any(ind in current_url for ind in login_indicators):
            return False

        # Negative: "Log på" button without 'loggedin' class = not logged in
        try:
            login_btn = self._page.query_selector("a.btn[ng-click*='loginUser']")
            if login_btn:
                btn_class = login_btn.get_attribute("class") or ""
                btn_text = login_btn.inner_text().strip()
                if "loggedin" not in btn_class and "Log på" in btn_text:
                    return False
                if "loggedin" in btn_class or "Log ud" in btn_text:
                    logger.debug("Login detected via button class/text")
                    return True
        except Exception:
            pass

        # Positive: lock icon is visible (ng-show="vm.userLoggedIn" without ng-hide)
        try:
            lock_icon = self._page.query_selector("span.icon-lock")
            if lock_icon:
                icon_class = lock_icon.get_attribute("class") or ""
                if "ng-hide" not in icon_class:
                    logger.debug("Login detected via visible lock icon")
                    return True
                else:
                    return False
        except Exception:
            pass

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
            input("Press Enter when you are logged in to sundhed.dk... ")
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
        logger.info("Browser closed")
