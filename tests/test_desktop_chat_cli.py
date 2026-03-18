"""Tests for DesktopAPI.chat() via Claude CLI subprocess.

Verifies that chat() uses subprocess (not anthropic SDK), passes health context
and system prompt, handles errors gracefully, and pushes responses to frontend.
"""

from __future__ import annotations

import random
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from health_platform.desktop.api import DesktopAPI

pytestmark = pytest.mark.integration


@pytest.fixture
def chat_db(tmp_path):
    """Temporary DuckDB with synthetic silver data and agent schema."""
    db_path = str(tmp_path / "test_chat_cli.db")
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")

    random.seed(42)
    days = [(date(2026, 3, 1) + timedelta(days=i)) for i in range(7)]

    # Sleep
    con.execute(
        """
        CREATE TABLE silver.daily_sleep (
            sk_date INTEGER, day DATE, sleep_score INTEGER,
            timestamp TIMESTAMP,
            contributor_deep_sleep INTEGER, contributor_efficiency INTEGER,
            contributor_latency INTEGER, contributor_rem_sleep INTEGER,
            contributor_restfulness INTEGER, contributor_timing INTEGER,
            contributor_total_sleep INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    for d in days:
        sk = int(d.strftime("%Y%m%d"))
        con.execute(
            "INSERT INTO silver.daily_sleep (sk_date, day, sleep_score) VALUES (?, ?, ?)",
            [sk, d, random.randint(60, 95)],
        )

    # Readiness
    con.execute(
        """
        CREATE TABLE silver.daily_readiness (
            sk_date INTEGER, day DATE, readiness_score INTEGER,
            timestamp TIMESTAMP,
            temperature_deviation DOUBLE, temperature_trend_deviation DOUBLE,
            contributor_activity_balance INTEGER, contributor_body_temperature INTEGER,
            contributor_hrv_balance INTEGER, contributor_previous_day_activity INTEGER,
            contributor_previous_night INTEGER, contributor_recovery_index INTEGER,
            contributor_resting_heart_rate INTEGER, contributor_sleep_balance INTEGER,
            contributor_sleep_regularity INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    for d in days:
        sk = int(d.strftime("%Y%m%d"))
        con.execute(
            "INSERT INTO silver.daily_readiness (sk_date, day, readiness_score) VALUES (?, ?, ?)",
            [sk, d, random.randint(55, 90)],
        )

    # Activity
    con.execute(
        """
        CREATE TABLE silver.daily_activity (
            sk_date INTEGER, day DATE, activity_score INTEGER,
            timestamp TIMESTAMP, steps INTEGER,
            total_calories INTEGER, active_calories INTEGER, target_calories INTEGER,
            average_met_minutes DOUBLE, equivalent_walking_distance INTEGER,
            high_activity_time INTEGER, medium_activity_time INTEGER,
            low_activity_time INTEGER, sedentary_time INTEGER,
            resting_time INTEGER, non_wear_time INTEGER,
            high_activity_met_minutes INTEGER, medium_activity_met_minutes INTEGER,
            low_activity_met_minutes INTEGER, sedentary_met_minutes INTEGER,
            inactivity_alerts INTEGER, target_meters INTEGER, meters_to_target INTEGER,
            contributor_meet_daily_targets INTEGER, contributor_move_every_hour INTEGER,
            contributor_recovery_time INTEGER, contributor_stay_active INTEGER,
            contributor_training_frequency INTEGER, contributor_training_volume INTEGER,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    for d in days:
        sk = int(d.strftime("%Y%m%d"))
        con.execute(
            "INSERT INTO silver.daily_activity (sk_date, day, steps) VALUES (?, ?, ?)",
            [sk, d, random.randint(5000, 13000)],
        )

    # Heart rate
    con.execute(
        """
        CREATE TABLE silver.heart_rate (
            sk_date INTEGER, sk_time VARCHAR,
            timestamp TIMESTAMP, bpm INTEGER, source_name VARCHAR,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    for d in days:
        sk = int(d.strftime("%Y%m%d"))
        con.execute(
            "INSERT INTO silver.heart_rate (sk_date, timestamp, bpm, source_name) VALUES (?, ?, ?, ?)",
            [sk, f"{d} 03:00:00", random.randint(48, 60), "Oura Ring"],
        )

    # Chat history table
    con.execute(
        """
        CREATE TABLE agent.chat_history (
            id VARCHAR PRIMARY KEY,
            session_id VARCHAR,
            role VARCHAR NOT NULL,
            content TEXT NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    con.close()
    return db_path


@pytest.fixture
def api(chat_db):
    """DesktopAPI instance with test database."""
    return DesktopAPI(chat_db)


@pytest.fixture
def mock_subprocess():
    """Patch subprocess.Popen to simulate Claude CLI."""
    mock_process = MagicMock()
    mock_process.communicate.return_value = ("CLI response about health", "")
    mock_process.returncode = 0

    with patch("health_platform.desktop.api.subprocess") as mock_mod:
        mock_mod.Popen.return_value = mock_process
        mock_mod.PIPE = -1
        yield mock_mod


class TestChatViaCLI:
    """Tests for chat() using Claude CLI subprocess."""

    def test_chat_calls_subprocess_not_anthropic(self, api, mock_subprocess):
        """chat() must use subprocess.Popen, not anthropic SDK."""
        api.chat("How is my sleep?")

        mock_subprocess.Popen.assert_called_once()
        cmd = mock_subprocess.Popen.call_args[0][0]
        assert cmd[0] == "claude"
        assert "-p" in cmd

    def test_chat_passes_health_context_to_cli(self, api, mock_subprocess):
        """The prompt sent to CLI must include health_data tags."""
        api.chat("How is my sleep?")

        process = mock_subprocess.Popen.return_value
        input_text = process.communicate.call_args[1]["input"]
        assert "<health_data>" in input_text
        assert "</health_data>" in input_text

    def test_chat_passes_system_prompt_to_cli(self, api, mock_subprocess):
        """The prompt sent to CLI must include SYSTEM_PROMPT content."""
        api.chat("What is my readiness?")

        process = mock_subprocess.Popen.return_value
        input_text = process.communicate.call_args[1]["input"]
        assert "personal health assistant" in input_text

    def test_chat_returns_cli_stdout_as_response(self, api, mock_subprocess):
        """chat() must return the CLI stdout as its response."""
        result = api.chat("How many steps?")
        assert result == "CLI response about health"

    def test_chat_saves_user_message_before_cli_call(self, api, mock_subprocess):
        """User message must be persisted in agent.chat_history."""
        api.chat("Test question")

        # Query via the write connection (read-only can't see uncommitted writes)
        con = api._get_write_connection()
        rows = con.execute(
            "SELECT role, content FROM agent.chat_history WHERE role = 'user'"
        ).fetchall()
        assert len(rows) >= 1
        assert rows[0][1] == "Test question"

    def test_chat_saves_assistant_response_after_cli_call(self, api, mock_subprocess):
        """CLI response must be persisted as assistant message."""
        api.chat("Test question")

        con = api._get_write_connection()
        rows = con.execute(
            "SELECT role, content FROM agent.chat_history WHERE role = 'assistant'"
        ).fetchall()
        assert len(rows) >= 1
        assert rows[0][1] == "CLI response about health"

    def test_chat_handles_cli_error_gracefully(self, api, mock_subprocess):
        """Non-zero returncode must produce a user-friendly error message."""
        process = mock_subprocess.Popen.return_value
        process.communicate.return_value = ("", "claude: model error")
        process.returncode = 1

        result = api.chat("Will this fail?")
        assert "error" in result.lower() or "Error" in result

    def test_chat_handles_cli_not_found(self, api):
        """FileNotFoundError from Popen must return a friendly message."""
        with patch("health_platform.desktop.api.subprocess") as mock_mod:
            mock_mod.Popen.side_effect = FileNotFoundError("claude not found")
            mock_mod.PIPE = -1

            result = api.chat("Hello?")
            assert "claude" in result.lower()

    def test_chat_pushes_response_to_frontend(self, api, mock_subprocess):
        """chat() must call evaluate_js with appendStreamChunk and finishStream."""
        mock_window = MagicMock()
        api.set_window(mock_window)

        api.chat("How am I doing?")

        js_calls = [c[0][0] for c in mock_window.evaluate_js.call_args_list]
        has_append = any("appendStreamChunk" in c for c in js_calls)
        has_finish = any("finishStream" in c for c in js_calls)
        assert has_append, "Expected appendStreamChunk call"
        assert has_finish, "Expected finishStream call"

    def test_chat_without_window_still_returns_response(self, api, mock_subprocess):
        """chat() must work even when _window is None (no GUI)."""
        api._window = None
        result = api.chat("Headless question")
        assert result == "CLI response about health"
