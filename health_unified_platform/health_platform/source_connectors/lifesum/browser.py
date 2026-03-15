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

# Keychain service names for credentials
KEYCHAIN_SERVICE_APPLE = "Apple id login"
KEYCHAIN_SERVICE_EMAIL = "Lifesum login"
KEYCHAIN_PATH = Path.home() / "Library" / "Keychains" / "claude.keychain-db"

# URL fragments that indicate a logged-in state
LOGGED_IN_URL_FRAGMENTS = ["/account", "/profile", "/dashboard", "/diary", "/home"]

# URL fragments that indicate still on login/auth pages
LOGIN_URL_FRAGMENTS = ["/login", "/signup", "/register", "/auth", "/oauth"]

# Auth detection
AUTH_POLL_INTERVAL_S = 2.0
AUTH_TIMEOUT_S = 180.0

# Selectors that indicate a logged-in state (verified from live site 2026-03-15)
AUTH_LOGGED_IN_SELECTORS = [
    "a:has-text('LOG AF')",
    "a:has-text('MIN KONTO')",
    "button:has-text('Log af')",
    "a:has-text('Min konto')",
    "a[href*='/account']",
    ".user-avatar",
    "[data-testid='user-menu']",
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
                KEYCHAIN_SERVICE_APPLE,
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
                KEYCHAIN_SERVICE_APPLE,
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
        self._captured_token: str | None = None

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
            args=[
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        self._context = self._browser
        self._page = self._context.new_page()

        # Intercept API requests to capture the JWT Bearer token
        self._page.on("request", self._intercept_token)

        # Strategy 1: Check existing session by navigating to a protected page.
        # Auth is stored in localStorage, not cookies — so we must load the SPA
        # to check. /account redirects to login if not authenticated.
        logger.info("Checking existing session via /account...")
        self._page.goto("https://lifesum.com/account", wait_until="domcontentloaded")
        self._page.wait_for_timeout(3000)

        if self._is_logged_in():
            logger.info("Already logged in (session reuse via localStorage)")
            return self._page

        # Not logged in — go to landing page for login flow
        logger.info("Not authenticated — navigating to %s", LOGIN_URL)
        self._page.goto(LOGIN_URL, wait_until="domcontentloaded")
        self._page.wait_for_timeout(3000)

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

    def _dismiss_cookie_consent(self) -> None:
        """Dismiss the Cookiebot consent dialog if present."""
        try:
            deny_btn = self._page.query_selector('button:has-text("Deny")')
            if not deny_btn:
                deny_btn = self._page.query_selector(
                    'button:has-text("Allow selection")'
                )
            if deny_btn:
                deny_btn.click()
                self._page.wait_for_timeout(1000)
                logger.info("Cookie consent dismissed")
        except Exception:
            pass

    def _try_automated_login(self) -> bool:
        """Attempt to log in via Sign in with Apple (primary) or email (fallback).

        Lifesum login flow (as of 2026-03):
        1. Landing page has a "Log på" button (opens popover)
        2. Popover has: Apple / Google / Facebook / email login options
        3. "Log på med Apple" opens appleid.apple.com popup
        4. Fill Apple ID credentials in popup → popup closes → authenticated

        Keychain entry "Lifesum login" stores Apple ID email + password.
        """
        if self._page is None:
            return False

        credentials = _get_credentials()
        if not credentials:
            logger.info("No credentials in keychain — skipping automated login")
            return False

        email, password = credentials

        try:
            # Dismiss cookie consent (blocks interaction otherwise)
            self._dismiss_cookie_consent()

            # Step 1: Open login popover — "Log på" is a <button>, not <a>
            login_btn = (
                self._page.query_selector("button:has-text('Log på')")
                or self._page.query_selector("a:has-text('Log på')")
                or self._page.query_selector("button:has-text('Log in')")
                or self._page.query_selector("[href*='login']")
            )
            if login_btn:
                logger.info("Clicking login button...")
                login_btn.click()
                self._page.wait_for_timeout(2000)

            # Step 2: Click "Log på med Apple" — opens Apple ID popup
            apple_btn = self._page.query_selector(
                "button:has-text('Log på med Apple')"
            ) or self._page.query_selector("button:has-text('Sign in with Apple')")
            if not apple_btn:
                logger.warning("Apple login button not found — trying email fallback")
                return self._try_email_login(email, password)

            # Catch the Apple ID popup
            logger.info("Clicking 'Log på med Apple'...")
            with self._page.context.expect_page(timeout=10000) as popup_info:
                apple_btn.click()

            apple_page = popup_info.value
            apple_page.wait_for_load_state("domcontentloaded")
            logger.info("Apple ID popup opened: %s", apple_page.url[:60])

            # Step 3: Fill Apple ID credentials in popup
            # Apple shows account_name first, then password after submit
            account_field = apple_page.wait_for_selector(
                "#account_name_text_field", timeout=10000
            )
            if not account_field:
                logger.warning("Apple ID account field not found")
                return False

            logger.info("Filling Apple ID email...")
            account_field.fill(email)

            # Submit email to proceed to authentication options
            continue_btn = apple_page.query_selector(
                "#sign-in"
            ) or apple_page.query_selector("button[type='submit']")
            if continue_btn:
                continue_btn.click()
            else:
                account_field.press("Enter")
            apple_page.wait_for_timeout(3000)

            # Try password login first (passkey requires Touch ID — not automatable)
            # Apple may show passkey prompt first — look for "Use password" link
            use_pw_link = (
                apple_page.query_selector("button:has-text('Use a Password')")
                or apple_page.query_selector("button:has-text('Sign In with Password')")
                or apple_page.query_selector("button:has-text('Brug en adgangskode')")
                or apple_page.query_selector(
                    "button:has-text('Log ind med adgangskode')"
                )
                or apple_page.query_selector("a:has-text('Use a Password')")
                or apple_page.query_selector("a:has-text('Sign In with Password')")
                or apple_page.query_selector("#password-login-link")
            )
            if use_pw_link:
                logger.info("Clicking 'Use password' link to bypass passkey...")
                use_pw_link.click()
                apple_page.wait_for_timeout(2000)

            # Now look for the password field
            password_field = apple_page.query_selector("#password_text_field")
            if not password_field:
                password_field = apple_page.wait_for_selector(
                    "#password_text_field", timeout=5000
                )
            if password_field:
                logger.info("Filling Apple ID password...")
                password_field.fill(password)
                sign_in_btn = apple_page.query_selector(
                    "#sign-in"
                ) or apple_page.query_selector("button[type='submit']")
                if sign_in_btn:
                    sign_in_btn.click()
                else:
                    password_field.press("Enter")
            else:
                # Password field not available — fall back to passkey (needs Touch ID)
                logger.warning(
                    "Password field not found — falling back to passkey (requires Touch ID)"
                )
                passkey_btn = apple_page.query_selector("#swp")
                if passkey_btn:
                    logger.info("Clicking passkey button (Touch ID required)...")
                    passkey_btn.click()
                    apple_page.wait_for_timeout(5000)
                else:
                    logger.warning("Neither password field nor passkey button found")
                    return False

            # Wait for Apple to authenticate and popup to close
            logger.info("Waiting for Apple authentication...")
            try:
                apple_page.wait_for_event("close", timeout=30000)
                logger.info("Apple popup closed — authentication complete")
            except Exception:
                # Popup might still be open (2FA prompt, etc.)
                if not apple_page.is_closed():
                    logger.warning(
                        "Apple popup still open — may need 2FA. URL: %s",
                        apple_page.url[:80],
                    )
                    # Wait a bit more for manual 2FA if needed
                    try:
                        apple_page.wait_for_event("close", timeout=30000)
                    except Exception:
                        logger.warning("Apple popup did not close after 2FA wait")
                        return False

            # Back on main page — navigate to account page to confirm auth
            self._page.wait_for_timeout(3000)
            self._page.goto(
                "https://lifesum.com/account", wait_until="domcontentloaded"
            )
            self._page.wait_for_timeout(3000)
            return self._is_logged_in()

        except Exception as exc:
            logger.warning("Automated login failed: %s", exc)
            return False

    def _try_email_login(self, email: str, password: str) -> bool:
        """Fallback: log in via email/password form."""
        try:
            email_login_btn = self._page.query_selector(
                "button:has-text('Log på med mailadresse')"
            ) or self._page.query_selector("button:has-text('Continue with email')")
            if email_login_btn:
                logger.info("Clicking 'Log på med mailadresse'...")
                email_login_btn.click()
                self._page.wait_for_timeout(2000)

            email_input = self._page.query_selector(
                "#emailInput"
            ) or self._page.query_selector("input[type='email']")
            if not email_input:
                logger.warning("Email input not found")
                return False

            email_input.fill(email)

            password_input = self._page.query_selector(
                "#passwordInput"
            ) or self._page.query_selector("input[type='password']")
            if not password_input:
                logger.warning("Password input not found")
                return False

            password_input.fill(password)

            submit_btn = self._page.query_selector("button[type='submit']")
            if submit_btn:
                submit_btn.click()
            else:
                password_input.press("Enter")

            self._page.wait_for_timeout(5000)
            return self._is_logged_in()

        except Exception as exc:
            logger.warning("Email login failed: %s", exc)
            return False

    def _is_logged_in(self) -> bool:
        """Check if the current page indicates a logged-in state.

        Lifesum is a SPA that may show a login overlay on top of
        authenticated routes like /account. We must check the DOM
        for login buttons as a negative signal, not just the URL.
        """
        if self._page is None:
            return False

        current_url = self._page.url.lower()

        # Negative: still on a login/auth page
        if any(frag in current_url for frag in LOGIN_URL_FRAGMENTS):
            return False

        # Negative: SPA login overlay present (buttons like "Log på med Apple")
        login_overlay_selectors = [
            "button:has-text('Log på med Apple')",
            "button:has-text('Log på med Facebook')",
            "button:has-text('Log på med Google')",
            "button:has-text('Sign in with Apple')",
        ]
        for sel in login_overlay_selectors:
            try:
                if self._page.query_selector(sel):
                    logger.debug("Login overlay detected via: %s", sel)
                    return False
            except Exception:
                continue

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

    def _intercept_token(self, request) -> None:
        """Capture JWT Bearer token from Lifesum API requests."""
        if "api.lifesum.com" not in request.url:
            return
        auth = request.headers.get("authorization", "")
        if auth.startswith("Bearer ") and len(auth) > 20:
            token = auth[7:]
            if self._captured_token != token:
                self._captured_token = token
                logger.info("Captured new JWT from API request to %s", request.url[:60])

    @property
    def captured_token(self) -> str | None:
        """Return the JWT captured from network interception, if any."""
        return self._captured_token

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
