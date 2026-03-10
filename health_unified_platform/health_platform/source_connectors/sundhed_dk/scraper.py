"""Page navigation and HTML extraction for sundhed.dk sections.

Uses a Playwright Page to navigate between data sections,
handle pagination, and extract raw HTML for the parsers.
"""

from __future__ import annotations

import random
import time

from health_platform.utils.logging_config import get_logger
from playwright.sync_api import Page
from playwright.sync_api import TimeoutError as PlaywrightTimeout

logger = get_logger("sundhed_dk.scraper")

# ---------------------------------------------------------------------------
# Navigation URLs — update when sundhed.dk restructures
# ---------------------------------------------------------------------------
BASE_URL = "https://www.sundhed.dk/borger"

SECTION_URLS = {
    "lab_results": f"{BASE_URL}/min-side/min-sundhedsjournal/laboratoriesvar/",
    "medications": f"{BASE_URL}/min-side/min-sundhedsjournal/medicinkortet/",
    "vaccinations": f"{BASE_URL}/min-side/min-sundhedsjournal/vaccinationer/",
    "ejournal": f"{BASE_URL}/min-side/min-sundhedsjournal/journal-fra-sygehus/",
    "appointments": f"{BASE_URL}/min-side/min-sundhedsjournal/henvisninger/",
}

# Pagination selectors
SEL_NEXT_PAGE = "a.next-page, button.next-page, [aria-label='Næste side']"
SEL_SHOW_MORE = "button.show-more, a.vis-mere, [data-action='show-more']"

# Content container — the main content area after navigation
# sundhed.dk uses AngularJS with custom directives
SEL_CONTENT_AREA = ".main-content .content, #content, .main-content, main"

# Session expiry indicators
SEL_LOGIN_PAGE = "input[name='username'], .mitid-login, #mitid-container"

# Delays between page loads (seconds) — polite scraping
MIN_DELAY_S = 1.0
MAX_DELAY_S = 3.0


class SessionExpiredError(Exception):
    """Raised when the sundhed.dk session has expired mid-scrape."""


class SundhedDkScraper:
    """Scrapes data sections from sundhed.dk using an authenticated Page.

    Usage:
        scraper = SundhedDkScraper(page)
        lab_html = scraper.scrape_section("lab_results")
        med_html = scraper.scrape_section("medications")
    """

    def __init__(self, page: Page) -> None:
        self._page = page

    def scrape_section(self, section: str) -> str:
        """Navigate to a section and extract all paginated HTML content.

        Args:
            section: One of 'lab_results', 'medications', 'vaccinations',
                     'ejournal', 'appointments'.

        Returns:
            Combined HTML string of all pages in the section.

        Raises:
            SessionExpiredError: If redirected to login page.
            ValueError: If section name is unknown.
        """
        if section not in SECTION_URLS:
            raise ValueError(
                f"Unknown section '{section}'. Valid sections: {list(SECTION_URLS.keys())}"
            )

        url = SECTION_URLS[section]
        logger.info("Scraping section '%s' from %s", section, url)

        self._polite_delay()
        self._page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        self._check_session()

        # Wait for content to load
        self._wait_for_content()

        # Collect HTML from all pages
        all_html_parts: list[str] = []
        page_num = 1

        while True:
            content = self._extract_content_html()
            all_html_parts.append(content)
            logger.info("  Page %d: extracted %d chars", page_num, len(content))

            if not self._go_to_next_page():
                break

            page_num += 1
            self._check_session()

        combined = "\n".join(all_html_parts)
        logger.info(
            "Section '%s': %d pages, %d total chars",
            section,
            page_num,
            len(combined),
        )
        return combined

    def scrape_all_sections(self) -> dict[str, str]:
        """Scrape all available sections.

        Returns:
            Dict mapping section name to HTML content.
        """
        results: dict[str, str] = {}

        for section in SECTION_URLS:
            try:
                results[section] = self.scrape_section(section)
            except SessionExpiredError:
                logger.error(
                    "Session expired during '%s'. Returning %d sections scraped so far.",
                    section,
                    len(results),
                )
                raise
            except Exception as exc:
                logger.warning("Failed to scrape '%s': %s", section, exc)
                results[section] = ""

        return results

    def _check_session(self) -> None:
        """Check if we have been redirected to a login page."""
        try:
            if self._page.query_selector(SEL_LOGIN_PAGE):
                raise SessionExpiredError(
                    "Session expired — redirected to login page. Please re-authenticate with MitID."
                )
        except SessionExpiredError:
            raise
        except Exception:
            pass

        if "login" in self._page.url.lower() or "mitid" in self._page.url.lower():
            raise SessionExpiredError(
                f"Session expired — current URL suggests login: {self._page.url}"
            )

    def _wait_for_content(self) -> None:
        """Wait for the main content area to appear."""
        try:
            self._page.wait_for_selector(
                SEL_CONTENT_AREA, state="attached", timeout=10_000
            )
        except PlaywrightTimeout:
            logger.warning("Content area not found within timeout, proceeding anyway")

    def _extract_content_html(self) -> str:
        """Extract HTML from the main content area."""
        for selector in SEL_CONTENT_AREA.split(", "):
            element = self._page.query_selector(selector.strip())
            if element:
                return element.inner_html()

        # Fallback: return full page HTML
        logger.warning("No content container found, using full page body")
        body = self._page.query_selector("body")
        return body.inner_html() if body else ""

    def _go_to_next_page(self) -> bool:
        """Try to navigate to the next page. Returns True if successful."""
        # First try "show more" button (loads content inline)
        for selector in SEL_SHOW_MORE.split(", "):
            btn = self._page.query_selector(selector.strip())
            if btn and btn.is_visible():
                try:
                    btn.click()
                    self._polite_delay()
                    self._page.wait_for_load_state("networkidle", timeout=10_000)
                    return True
                except Exception as exc:
                    logger.debug("Show-more click failed: %s", exc)

        # Then try "next page" link
        for selector in SEL_NEXT_PAGE.split(", "):
            link = self._page.query_selector(selector.strip())
            if link and link.is_visible():
                try:
                    link.click()
                    self._polite_delay()
                    self._page.wait_for_load_state("domcontentloaded", timeout=10_000)
                    return True
                except Exception as exc:
                    logger.debug("Next-page click failed: %s", exc)

        return False

    @staticmethod
    def _polite_delay() -> None:
        """Random delay between page loads to be respectful."""
        delay = random.uniform(MIN_DELAY_S, MAX_DELAY_S)
        time.sleep(delay)
