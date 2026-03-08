"""py2app setup configuration for HealthReporting.

Usage:
    python setup_py2app.py py2app
"""

from pathlib import Path

from setuptools import setup

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DESKTOP_DIR = _PROJECT_ROOT / "health_platform" / "desktop"
_UI_DIR = _DESKTOP_DIR / "ui"
_TEMPLATES_DIR = _DESKTOP_DIR / "reports" / "templates"

APP_NAME = "HealthReporting"
BUNDLE_ID = "com.healthreporting.desktop"

APP = [str(_DESKTOP_DIR / "__main__.py")]

DATA_FILES = [
    (
        "health_platform/desktop/ui",
        list(str(p) for p in _UI_DIR.rglob("*") if p.is_file()),
    ),
    (
        "health_platform/desktop/reports/templates",
        list(str(p) for p in _TEMPLATES_DIR.rglob("*") if p.is_file()),
    ),
]

OPTIONS = {
    "argv_emulation": False,
    "iconfile": None,
    "plist": {
        "CFBundleName": APP_NAME,
        "CFBundleDisplayName": APP_NAME,
        "CFBundleIdentifier": BUNDLE_ID,
        "CFBundleVersion": "0.1.0",
        "CFBundleShortVersionString": "0.1.0",
        "LSMinimumSystemVersion": "12.0",
        "NSHighResolutionCapable": True,
        "LSBackgroundOnly": False,
    },
    "packages": [
        "duckdb",
        "jinja2",
        "weasyprint",
        "webview",
        "anthropic",
        "health_platform",
    ],
    "includes": [
        "health_platform.desktop",
        "health_platform.desktop.api",
        "health_platform.desktop.app",
        "health_platform.desktop.reports",
        "health_platform.desktop.reports.generator",
        "health_platform.mcp.health_tools",
        "health_platform.mcp.query_builder",
        "health_platform.utils.logging_config",
        "health_platform.utils.keychain",
    ],
}

setup(
    name=APP_NAME,
    app=APP,
    data_files=DATA_FILES,
    options={"py2app": OPTIONS},
    setup_requires=["py2app"],
)
