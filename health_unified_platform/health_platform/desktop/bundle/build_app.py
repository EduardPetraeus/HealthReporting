"""Build macOS .app bundle for HealthReporting.

Supports both PyInstaller and py2app build methods.
Usage:
    python build_app.py [--method pyinstaller|py2app]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

# Paths
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DESKTOP_DIR = _PROJECT_ROOT / "health_platform" / "desktop"
_UI_DIR = _DESKTOP_DIR / "ui"
_TEMPLATES_DIR = _DESKTOP_DIR / "reports" / "templates"
_ENTRY_POINT = _DESKTOP_DIR / "__main__.py"

APP_NAME = "HealthReporting"
BUNDLE_ID = "com.healthreporting.desktop"


def build_pyinstaller() -> None:
    """Build .app using PyInstaller."""
    print(f"Building {APP_NAME} with PyInstaller...")

    dist_dir = _PROJECT_ROOT / "dist"
    build_dir = _PROJECT_ROOT / "build"

    cmd = [
        sys.executable,
        "-m",
        "PyInstaller",
        "--name",
        APP_NAME,
        "--windowed",
        "--onedir",
        "--noconfirm",
        # Add data files
        "--add-data",
        f"{_UI_DIR}:health_platform/desktop/ui",
        "--add-data",
        f"{_TEMPLATES_DIR}:health_platform/desktop/reports/templates",
        # Hidden imports needed by the app
        "--hidden-import",
        "duckdb",
        "--hidden-import",
        "jinja2",
        "--hidden-import",
        "weasyprint",
        "--hidden-import",
        "webview",
        "--hidden-import",
        "anthropic",
        # macOS bundle config
        "--osx-bundle-identifier",
        BUNDLE_ID,
        # Paths
        "--distpath",
        str(dist_dir),
        "--workpath",
        str(build_dir),
        # Entry point
        str(_ENTRY_POINT),
    ]

    result = subprocess.run(cmd, cwd=str(_PROJECT_ROOT), check=False)
    if result.returncode == 0:
        app_path = dist_dir / f"{APP_NAME}.app"
        print(f"Build successful: {app_path}")
    else:
        print("Build failed. Check output above for errors.")
        sys.exit(1)


def build_py2app() -> None:
    """Build .app using py2app via the setup script."""
    print(f"Building {APP_NAME} with py2app...")

    setup_script = Path(__file__).parent / "setup_py2app.py"
    if not setup_script.exists():
        print(f"Error: {setup_script} not found")
        sys.exit(1)

    cmd = [sys.executable, str(setup_script), "py2app"]
    result = subprocess.run(cmd, cwd=str(_PROJECT_ROOT), check=False)
    if result.returncode == 0:
        print(f"Build successful: dist/{APP_NAME}.app")
    else:
        print("Build failed. Check output above for errors.")
        sys.exit(1)


def main() -> None:
    """Parse arguments and build the app."""
    parser = argparse.ArgumentParser(description=f"Build {APP_NAME} macOS .app bundle")
    parser.add_argument(
        "--method",
        choices=["pyinstaller", "py2app"],
        default="pyinstaller",
        help="Build method to use (default: pyinstaller)",
    )
    args = parser.parse_args()

    if args.method == "pyinstaller":
        build_pyinstaller()
    else:
        build_py2app()


if __name__ == "__main__":
    main()
