"""One-shot live scrape script for sundhed.dk.

Run from repo root:
    HEALTH_ENV=dev .venv/bin/python health_unified_platform/health_platform/source_connectors/sundhed_dk/live_scrape.py

Browser should already be open and authenticated via MitID.
If not, the script will open a browser and wait for login.
"""

import sys  # noqa: E402
from pathlib import Path  # noqa: E402

# Add package paths — must happen before local imports
_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_root))
sys.path.insert(0, str(_root.parent))

from source_connectors.sundhed_dk.browser import SundhedDkBrowser  # noqa: E402
from source_connectors.sundhed_dk.parsers import (  # noqa: E402
    parse_appointments,
    parse_ejournal,
    parse_lab_results,
    parse_medications,
    parse_vaccinations,
)
from source_connectors.sundhed_dk.scraper import (  # noqa: E402
    SundhedDkScraper,
    save_html,
)
from source_connectors.sundhed_dk.validator import (  # noqa: E402
    generate_validation_report,
    validate_parse_completeness,
)

ARCHIVE_ROOT = "/Users/Shared/data_lake/min sundhed"
DEBUG_DIR = Path(ARCHIVE_ROOT) / "debug"

SECTIONS = [
    ("lab_results", parse_lab_results),
    ("medications", parse_medications),
    ("vaccinations", parse_vaccinations),
    ("ejournal", parse_ejournal),
    ("appointments", parse_appointments),
]


def main():
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    browser = SundhedDkBrowser()
    reports = []

    try:
        page = browser.launch_and_authenticate()
        scraper = SundhedDkScraper(page)

        for section_name, parser_fn in SECTIONS:
            print(f"\n{'=' * 60}")
            print(f"Scraping: {section_name}")
            print(f"{'=' * 60}")

            try:
                html = scraper.scrape_section(section_name)
            except Exception as exc:
                print(f"  ERROR scraping {section_name}: {exc}")
                continue

            print(f"  HTML: {len(html)} chars")

            # Save debug HTML
            debug_file = DEBUG_DIR / f"{section_name}.html"
            debug_file.write_text(html, encoding="utf-8")
            print(f"  Debug HTML: {debug_file}")

            # Archive
            save_html(html, section_name, ARCHIVE_ROOT)

            # Parse
            records = parser_fn(html)
            print(f"  Parsed: {len(records)} records")

            # Validate
            report = validate_parse_completeness(html, records, section_name)
            reports.append(report)
            status = "OK" if report.match else "MISMATCH"
            print(
                f"  Validation: HTML={report.html_rows}, parsed={report.parsed_rows} -> {status}"
            )

            # Show first record
            if records:
                print(f"  Sample: {records[0]}")

    finally:
        browser.close()

    # Final report
    if reports:
        print(f"\n{'=' * 60}")
        print("VALIDATION REPORT")
        print(f"{'=' * 60}")
        print(generate_validation_report(reports))

        # Save report
        report_path = Path(ARCHIVE_ROOT) / "validation_report.txt"
        report_path.write_text(generate_validation_report(reports), encoding="utf-8")
        print(f"\nReport saved: {report_path}")


if __name__ == "__main__":
    main()
