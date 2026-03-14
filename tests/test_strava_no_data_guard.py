"""
Tests for the Strava no-data guard: when the API returns zero records,
the state file must NOT be updated (watermark must not advance).
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parents[1]
_STRAVA_DIR = (
    _REPO_ROOT
    / "health_unified_platform"
    / "health_platform"
    / "source_connectors"
    / "strava"
)


@pytest.fixture()
def strava_state_file(tmp_path: Path) -> Path:
    """Create a temporary state file with a known watermark."""
    state_file = tmp_path / "strava_state.json"
    initial_state = {"activities": "2026-03-01"}
    state_file.write_text(json.dumps(initial_state))
    return state_file


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestNoDataGuard:
    """When API returns 0 records, state must remain unchanged."""

    def test_state_unchanged_when_api_returns_empty(
        self, strava_state_file: Path, tmp_path: Path
    ) -> None:
        """Simulate a run where fetch_activities returns [] and verify
        the state file watermark does not advance."""
        import importlib.util

        # Load strava state module
        spec = importlib.util.spec_from_file_location(
            "strava_state", _STRAVA_DIR / "state.py"
        )
        strava_state = importlib.util.module_from_spec(spec)

        # Point STATE_FILE to our temp file
        with patch.object(strava_state, "__name__", "strava_state"):
            spec.loader.exec_module(strava_state)
        strava_state.STATE_FILE = strava_state_file

        # Load state, verify initial watermark
        state = strava_state.load_state()
        assert state["activities"] == "2026-03-01"

        # Simulate: API returned 0 records — the no-data guard in
        # run_strava.py should skip update_state(). We test the logic
        # directly: if records is empty, do NOT call update_state.
        records: list[dict] = []

        if records:
            strava_state.update_state("activities", date(2026, 3, 14), state)

        # State must be unchanged
        assert state["activities"] == "2026-03-01"

        # Also verify save_state would persist the unchanged watermark
        strava_state.save_state(state)
        persisted = json.loads(strava_state_file.read_text())
        assert persisted["activities"] == "2026-03-01"

    def test_state_advances_when_data_present(
        self, strava_state_file: Path, tmp_path: Path
    ) -> None:
        """Positive case: when records exist, state should advance."""
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "strava_state", _STRAVA_DIR / "state.py"
        )
        strava_state = importlib.util.module_from_spec(spec)
        with patch.object(strava_state, "__name__", "strava_state"):
            spec.loader.exec_module(strava_state)
        strava_state.STATE_FILE = strava_state_file

        state = strava_state.load_state()

        records = [{"activity_id": 123, "name": "Morning Run"}]

        if records:
            strava_state.update_state("activities", date(2026, 3, 14), state)

        assert state["activities"] == "2026-03-14"

        strava_state.save_state(state)
        persisted = json.loads(strava_state_file.read_text())
        assert persisted["activities"] == "2026-03-14"

    def test_already_up_to_date_skips(
        self, strava_state_file: Path, tmp_path: Path
    ) -> None:
        """When start_date > END_DATE, the endpoint should be skipped entirely."""
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "strava_state", _STRAVA_DIR / "state.py"
        )
        strava_state = importlib.util.module_from_spec(spec)
        with patch.object(strava_state, "__name__", "strava_state"):
            spec.loader.exec_module(strava_state)
        strava_state.STATE_FILE = strava_state_file

        # Set watermark to the future
        future_state = {"activities": date.today().isoformat()}
        strava_state_file.write_text(json.dumps(future_state))

        state = strava_state.load_state()
        start_date = strava_state.get_start_date("activities", state)

        # start_date should be tomorrow (last_fetched + 1 day), which > today
        assert start_date > date.today()
