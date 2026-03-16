"""Unit tests for run_oura per-endpoint error handling (Issue #197)."""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import HTTPError

# run_oura.py uses bare imports (from auth import ...) that only resolve
# when the oura connector directory is on sys.path.
_OURA_DIR = str(
    Path(__file__).resolve().parents[1]
    / "health_unified_platform"
    / "health_platform"
    / "source_connectors"
    / "oura"
)
if _OURA_DIR not in sys.path:
    sys.path.insert(0, _OURA_DIR)

from health_platform.source_connectors.oura.run_oura import (  # noqa: E402
    ENDPOINTS,
    main,
)


@pytest.fixture()
def mock_pipeline():
    """Set up mocked pipeline dependencies for run_oura.main()."""
    with (
        patch(
            "health_platform.source_connectors.oura.run_oura.get_access_token"
        ) as mock_auth,
        patch(
            "health_platform.source_connectors.oura.run_oura.OuraClient"
        ) as mock_client_cls,
        patch(
            "health_platform.source_connectors.oura.run_oura.load_state"
        ) as mock_load,
        patch(
            "health_platform.source_connectors.oura.run_oura.clean_state"
        ) as mock_clean,
        patch(
            "health_platform.source_connectors.oura.run_oura.get_start_date"
        ) as mock_start,
        patch(
            "health_platform.source_connectors.oura.run_oura.update_state"
        ) as mock_update,
        patch(
            "health_platform.source_connectors.oura.run_oura.write_records"
        ) as mock_write,
        patch(
            "health_platform.source_connectors.oura.run_oura.AuditLogger"
        ) as mock_audit_cls,
    ):
        mock_auth.return_value = "fake-token"
        mock_load.return_value = {}
        mock_clean.return_value = {}

        # All endpoints return yesterday as start date (not up-to-date)
        mock_start.return_value = date.today() - timedelta(days=1)

        client = MagicMock()
        mock_client_cls.return_value = client

        # Default: all fetch methods return empty list
        for _, method_name, _ in ENDPOINTS:
            getattr(client, method_name).return_value = []
        client.fetch_personal_info.return_value = {}

        audit = MagicMock()
        mock_audit_cls.return_value.__enter__ = MagicMock(return_value=audit)
        mock_audit_cls.return_value.__exit__ = MagicMock(return_value=False)

        yield {
            "client": client,
            "audit": audit,
            "update_state": mock_update,
            "write_records": mock_write,
        }


class TestPerEndpointErrorHandling:
    """Verify that a single endpoint failure does not crash the pipeline."""

    def test_single_404_pipeline_continues(self, mock_pipeline):
        """A 404 on vo2_max should not prevent other endpoints from running."""
        client = mock_pipeline["client"]
        response = MagicMock()
        response.status_code = 404
        client.fetch_vo2_max.side_effect = HTTPError(response=response)

        main()

        # Other endpoints still called (e.g. daily_sleep)
        client.fetch_daily_sleep.assert_called_once()
        # personal_info still called at the end
        client.fetch_personal_info.assert_called_once()

    def test_multiple_failures_non_failing_still_run(self, mock_pipeline):
        """Multiple failing endpoints don't block the rest."""
        client = mock_pipeline["client"]
        client.fetch_vo2_max.side_effect = HTTPError(
            response=MagicMock(status_code=404)
        )
        client.fetch_heartrate.side_effect = HTTPError(
            response=MagicMock(status_code=500)
        )
        client.fetch_session.side_effect = Exception("connection timeout")

        main()

        # Non-failing endpoints still called
        client.fetch_daily_sleep.assert_called_once()
        client.fetch_daily_activity.assert_called_once()
        client.fetch_daily_readiness.assert_called_once()

    def test_state_not_updated_for_failed_endpoint(self, mock_pipeline):
        """State should NOT be updated for endpoints that fail."""
        client = mock_pipeline["client"]
        client.fetch_vo2_max.side_effect = HTTPError(
            response=MagicMock(status_code=404)
        )

        main()

        update_calls = [
            call.args[0] for call in mock_pipeline["update_state"].call_args_list
        ]
        assert "vo2_max" not in update_calls

    def test_audit_logs_skipped_on_failure(self, mock_pipeline):
        """Audit logger should record status='skipped' for failed endpoints."""
        client = mock_pipeline["client"]
        client.fetch_vo2_max.side_effect = HTTPError(
            response=MagicMock(status_code=404)
        )

        main()

        audit = mock_pipeline["audit"]
        skipped_calls = [
            call
            for call in audit.log_table.call_args_list
            if call.kwargs.get("status") == "skipped"
            or (len(call.args) > 3 and call.args[3] == "skipped")
        ]
        assert len(skipped_calls) == 1
        assert "vo2_max" in skipped_calls[0].args[0]

    def test_personal_info_failure_does_not_crash(self, mock_pipeline):
        """A failure in personal_info should not crash the pipeline."""
        client = mock_pipeline["client"]
        client.fetch_personal_info.side_effect = Exception("connection refused")

        main()

        # All dated endpoints still ran successfully
        client.fetch_daily_sleep.assert_called_once()

        audit = mock_pipeline["audit"]
        skipped_calls = [
            call
            for call in audit.log_table.call_args_list
            if call.kwargs.get("status") == "skipped"
            or (len(call.args) > 3 and call.args[3] == "skipped")
        ]
        assert any("personal_info" in c.args[0] for c in skipped_calls)
