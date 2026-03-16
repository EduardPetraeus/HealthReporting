"""Unit tests for Oura state management — stale state warnings."""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import patch

from health_platform.source_connectors.oura.state import (
    STALE_THRESHOLD_DAYS,
    get_start_date,
)


class TestStaleStateWarning:
    """Verify stale state detection in get_start_date()."""

    def test_stale_state_triggers_warning(self):
        """State older than STALE_THRESHOLD_DAYS logs a warning."""
        old_date = (
            date.today() - timedelta(days=STALE_THRESHOLD_DAYS + 100)
        ).isoformat()
        state = {"daily_sleep": old_date}
        with patch(
            "health_platform.source_connectors.oura.state.logger"
        ) as mock_logger:
            get_start_date("daily_sleep", state)
            mock_logger.warning.assert_called_once()
            assert "Stale state" in mock_logger.warning.call_args[0][0]

    def test_fresh_state_no_warning(self):
        """State from yesterday does not trigger warning."""
        yesterday = (date.today() - timedelta(days=1)).isoformat()
        state = {"daily_sleep": yesterday}
        with patch(
            "health_platform.source_connectors.oura.state.logger"
        ) as mock_logger:
            result = get_start_date("daily_sleep", state)
            mock_logger.warning.assert_not_called()
            assert result == date.today()

    def test_boundary_365_no_warning(self):
        """State exactly at threshold (365 days) does NOT trigger warning (> not >=)."""
        boundary_date = (date.today() - timedelta(days=365)).isoformat()
        state = {"heartrate": boundary_date}
        with patch(
            "health_platform.source_connectors.oura.state.logger"
        ) as mock_logger:
            get_start_date("heartrate", state)
            mock_logger.warning.assert_not_called()

    def test_boundary_366_triggers_warning(self):
        """State at 366 days triggers warning (one past threshold)."""
        over_date = (date.today() - timedelta(days=366)).isoformat()
        state = {"heartrate": over_date}
        with patch(
            "health_platform.source_connectors.oura.state.logger"
        ) as mock_logger:
            get_start_date("heartrate", state)
            mock_logger.warning.assert_called_once()
