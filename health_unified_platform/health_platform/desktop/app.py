"""pywebview application launcher.

Creates a native macOS window (WKWebView) serving the desktop UI.
The DesktopAPI class is exposed to JavaScript as window.pywebview.api.
"""

from __future__ import annotations

import os
from pathlib import Path

import webview
from health_platform.desktop.api import DesktopAPI
from health_platform.utils.logging_config import get_logger

logger = get_logger("desktop.app")

_UI_DIR = Path(__file__).resolve().parent / "ui"


def _get_db_path() -> str:
    """Resolve DuckDB database path from environment."""
    if path := os.environ.get("HEALTH_DB_PATH"):
        return path
    env = os.environ.get("HEALTH_ENV", "dev")
    return str(Path.home() / "health_dw" / f"health_dw_{env}.db")


def launch() -> None:
    """Launch the desktop application window."""
    db_path = _get_db_path()
    logger.info("Starting desktop app. DB: %s", db_path)

    api = DesktopAPI(db_path)

    index_path = _UI_DIR / "index.html"
    if not index_path.exists():
        raise FileNotFoundError(f"UI not found: {index_path}")

    window = webview.create_window(
        title="HealthReporting",
        url=str(index_path),
        js_api=api,
        width=1200,
        height=800,
        min_size=(900, 600),
        background_color="#000000",
        text_select=True,
    )

    api.set_window(window)

    webview.start(debug=os.environ.get("HEALTH_DEBUG", "").lower() == "true")
