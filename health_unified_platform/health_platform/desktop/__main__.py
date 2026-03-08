"""Entry point for the desktop application.

Usage:
    python -m health_platform.desktop
    python -m health_platform.desktop --dev   # Seed a dev database first
"""

from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description="HealthReporting Desktop App")
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Seed a dev database with synthetic data before launching",
    )
    args = parser.parse_args()

    if args.dev:
        from health_platform.desktop.seed_dev_db import seed_dev_database

        seed_dev_database()

    from health_platform.desktop.app import launch

    launch()


if __name__ == "__main__":
    main()
