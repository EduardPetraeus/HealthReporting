"""
Tracks the last successfully fetched date per Oura endpoint.
Persisted to ~/.config/health_reporting/oura_state.json between runs.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

STATE_FILE = Path.home() / ".config" / "health_reporting" / "oura_state.json"
DEFAULT_LOOKBACK_DAYS = 90


def load_state() -> dict:
    """Loads persisted fetch state. Returns empty dict on first run."""
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(state: dict) -> None:
    """Persists fetch state to disk."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))
    print(f"State saved to {STATE_FILE}")


def get_start_date(endpoint: str, state: dict) -> date:
    """
    Returns the start date for fetching an endpoint.
    - First run: today minus DEFAULT_LOOKBACK_DAYS
    - Subsequent runs: day after the last fetched date
    """
    last_fetched = state.get(endpoint)
    if last_fetched:
        return date.fromisoformat(last_fetched) + timedelta(days=1)
    return date.today() - timedelta(days=DEFAULT_LOOKBACK_DAYS)


def update_state(endpoint: str, fetched_through: date, state: dict) -> None:
    """Updates the in-memory state for an endpoint. Call save_state() to persist."""
    state[endpoint] = fetched_through.isoformat()
