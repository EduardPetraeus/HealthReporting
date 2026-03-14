"""
Tracks the last successfully fetched date per Withings endpoint.
Persisted to ~/.config/health_reporting/withings_state.json between runs.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

from health_platform.utils.logging_config import get_logger

logger = get_logger("withings.state")

STATE_FILE = Path.home() / ".config" / "health_reporting" / "withings_state.json"
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
    logger.info("State saved to %s", STATE_FILE)


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
    """Updates the in-memory state for an endpoint and persists immediately."""
    state[endpoint] = fetched_through.isoformat()
    save_state(state)
    logger.info("State updated: %s -> %s", endpoint, fetched_through.isoformat())


def clean_state(valid_endpoints: list[str], state: dict) -> dict:
    """Removes ghost entries from state that no longer match active endpoints."""
    ghost_keys = [k for k in state if k not in valid_endpoints]
    for key in ghost_keys:
        logger.info("Removing ghost state entry: %s", key)
        del state[key]
    if ghost_keys:
        save_state(state)
    return state
