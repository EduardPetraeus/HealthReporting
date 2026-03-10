"""Tests for the DesktopAPI class.

Uses a temporary DuckDB database with synthetic data.
"""

from __future__ import annotations

import pytest
from health_platform.desktop.api import DesktopAPI, _sanitize


@pytest.fixture
def dev_db(tmp_path):
    """Create a temporary DuckDB with synthetic data and return path."""
    import duckdb

    db_path = str(tmp_path / "test_health.db")
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")

    # Sleep data
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
    con.execute(
        """
        INSERT INTO silver.daily_sleep (sk_date, day, sleep_score)
        VALUES
            (20260301, '2026-03-01', 82),
            (20260302, '2026-03-02', 75),
            (20260303, '2026-03-03', 91),
            (20260304, '2026-03-04', 58),
            (20260305, '2026-03-05', 88)
    """
    )

    # Readiness data
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
    con.execute(
        """
        INSERT INTO silver.daily_readiness (sk_date, day, readiness_score)
        VALUES
            (20260301, '2026-03-01', 79),
            (20260302, '2026-03-02', 72),
            (20260303, '2026-03-03', 88),
            (20260304, '2026-03-04', 55),
            (20260305, '2026-03-05', 85)
    """
    )

    # Activity data
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
    con.execute(
        """
        INSERT INTO silver.daily_activity (sk_date, day, activity_score, steps, active_calories, total_calories)
        VALUES
            (20260301, '2026-03-01', 91, 12450, 650, 2400),
            (20260302, '2026-03-02', 68, 5200, 320, 2100),
            (20260303, '2026-03-03', 85, 9800, 520, 2300),
            (20260304, '2026-03-04', 45, 3100, 180, 1900),
            (20260305, '2026-03-05', 78, 8500, 450, 2200)
    """
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
    con.execute(
        """
        INSERT INTO silver.heart_rate (sk_date, timestamp, bpm, source_name)
        VALUES
            (20260301, '2026-03-01 03:00:00', 52, 'Oura Ring'),
            (20260301, '2026-03-01 12:00:00', 78, 'Apple Watch'),
            (20260302, '2026-03-02 03:00:00', 55, 'Oura Ring'),
            (20260303, '2026-03-03 03:00:00', 50, 'Oura Ring'),
            (20260304, '2026-03-04 03:00:00', 72, 'Oura Ring'),
            (20260305, '2026-03-05 03:00:00', 51, 'Oura Ring')
    """
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
def api(dev_db):
    """Create a DesktopAPI instance with test database."""
    return DesktopAPI(dev_db)


# ===================================================================
# Helpers for chat tests
# ===================================================================


def _close_all(api):
    """Close all DuckDB connections to allow fresh connections."""
    if api._con:
        api._con.close()
        api._con = None
    if api._write_con:
        api._write_con.close()
        api._write_con = None


def _read_rows(
    db_path, sql="SELECT role, content FROM agent.chat_history ORDER BY timestamp"
):
    """Read rows from a fresh read-only connection."""
    import duckdb

    con = duckdb.connect(db_path, read_only=True)
    rows = con.execute(sql).fetchall()
    con.close()
    return rows


# ===================================================================
# Session 1: Dashboard & System Tests
# ===================================================================


class TestSanitize:
    def test_decimal_converted(self):
        from decimal import Decimal

        assert _sanitize(Decimal("42.5")) == 42.5

    def test_date_converted(self):
        from datetime import date

        assert _sanitize(date(2026, 3, 1)) == "2026-03-01"

    def test_nested_dict(self):
        from decimal import Decimal

        result = _sanitize({"score": Decimal("85.3"), "name": "test"})
        assert result == {"score": 85.3, "name": "test"}

    def test_list_sanitized(self):
        from decimal import Decimal

        result = _sanitize([Decimal("1.0"), Decimal("2.0")])
        assert result == [1.0, 2.0]

    def test_passthrough(self):
        assert _sanitize(42) == 42
        assert _sanitize("hello") == "hello"
        assert _sanitize(None) is None


class TestDashboardData:
    def test_returns_all_keys(self, api):
        data = api.get_dashboard_data()
        assert "health_score" in data
        assert "kpis" in data
        assert "sparklines" in data
        assert "trends" in data
        assert "alerts" in data
        assert "timestamp" in data

    def test_health_score(self, api):
        hs = api.get_dashboard_data()["health_score"]
        assert hs["score"] is not None
        assert hs["status"] in ("excellent", "good", "fair", "poor")
        assert hs["day"] is not None
        assert "sleep" in hs["components"]
        assert "readiness" in hs["components"]
        assert "activity" in hs["components"]

    def test_health_score_computation(self, api):
        hs = api.get_dashboard_data()["health_score"]
        comp = hs["components"]
        expected = round(
            (comp["sleep"] or 0) * 0.35
            + (comp["readiness"] or 0) * 0.35
            + (comp["activity"] or 0) * 0.30,
            1,
        )
        assert abs(hs["score"] - expected) < 0.2

    def test_kpis(self, api):
        kpis = api.get_dashboard_data()["kpis"]
        assert "sleep_score" in kpis
        assert "readiness_score" in kpis
        assert "steps" in kpis
        assert "resting_hr" in kpis

        for _key, kpi in kpis.items():
            assert "label" in kpi
            assert "value" in kpi
            assert "day" in kpi
            assert "unit" in kpi

    def test_kpi_values(self, api):
        kpis = api.get_dashboard_data()["kpis"]
        assert kpis["sleep_score"]["value"] == 88  # Latest (2026-03-05)
        assert kpis["readiness_score"]["value"] == 85
        assert kpis["steps"]["value"] == 8500
        assert kpis["resting_hr"]["value"] == 51

    def test_sparklines(self, api):
        sparklines = api.get_dashboard_data()["sparklines"]
        assert "sleep_score" in sparklines
        assert "readiness_score" in sparklines
        assert "steps" in sparklines
        assert "resting_hr" in sparklines

        for _key, series in sparklines.items():
            assert isinstance(series, list)
            for point in series:
                assert "day" in point
                assert "value" in point

    def test_trends(self, api):
        trends = api.get_dashboard_data()["trends"]
        assert "days" in trends
        assert "sleep" in trends
        assert "readiness" in trends
        assert len(trends["days"]) == len(trends["sleep"])
        assert len(trends["days"]) == len(trends["readiness"])

    def test_alerts_low_scores(self, api):
        alerts = api.get_dashboard_data()["alerts"]
        assert isinstance(alerts, list)
        alert_metrics = [a["metric"] for a in alerts]
        assert "sleep_score" in alert_metrics
        assert "readiness_score" in alert_metrics

    def test_alerts_elevated_hr(self, api):
        alerts = api.get_dashboard_data()["alerts"]
        hr_alerts = [a for a in alerts if a["metric"] == "resting_hr"]
        assert len(hr_alerts) == 1
        assert "72" in hr_alerts[0]["message"]


class TestNoDatabase:
    def test_missing_db(self, tmp_path):
        missing_api = DesktopAPI(str(tmp_path / "nonexistent.db"))
        data = missing_api.get_dashboard_data()
        assert data["error"] == "no_database"


class TestQueryMetric:
    def test_query_returns_result(self, api):
        result = api.query_metric("sleep_score", "daily_value", "last_7_days")
        assert result["error"] is None
        assert result["result"] is not None


class TestListMetrics:
    def test_returns_metrics(self, api):
        metrics = api.list_metrics()
        assert isinstance(metrics, list)
        assert "sleep_score" in metrics
        assert "readiness_score" in metrics
        assert "steps" in metrics


class TestGetStatus:
    def test_status_with_db(self, api):
        status = api.get_status()
        assert status["db_exists"] is True
        assert status["db_size_mb"] > 0

    def test_status_without_db(self, tmp_path):
        missing_api = DesktopAPI(str(tmp_path / "nonexistent.db"))
        status = missing_api.get_status()
        assert status["db_exists"] is False
        assert status["db_size_mb"] == 0


# ===================================================================
# Session 2: Chat Integration Tests
# ===================================================================


class TestChatHistoryTable:
    def test_ensure_chat_history_table(self, api):
        """_ensure_chat_history_table creates the table if needed."""
        import duckdb

        _close_all(api)
        con = duckdb.connect(api._db_path)
        con.execute("DROP TABLE IF EXISTS agent.chat_history")
        con.close()

        api._get_write_connection()
        _close_all(api)

        rows = _read_rows(api._db_path, "SELECT COUNT(*) FROM agent.chat_history")
        assert rows[0][0] == 0


class TestSaveChatMessage:
    def test_save_user_message(self, api):
        """_save_chat_message persists a user message."""
        api._save_chat_message("user", "How did I sleep?")
        session_id = api._session_id
        _close_all(api)

        rows = _read_rows(
            api._db_path,
            "SELECT role, content, session_id FROM agent.chat_history",
        )
        assert len(rows) == 1
        assert rows[0][0] == "user"
        assert rows[0][1] == "How did I sleep?"
        assert rows[0][2] == session_id

    def test_save_assistant_message(self, api):
        """_save_chat_message persists an assistant message."""
        api._save_chat_message("assistant", "Your sleep score was 88.")
        _close_all(api)

        rows = _read_rows(api._db_path)
        assert len(rows) == 1
        assert rows[0][0] == "assistant"
        assert rows[0][1] == "Your sleep score was 88."

    def test_save_multiple_messages(self, api):
        """Multiple messages get unique IDs and correct ordering."""
        api._save_chat_message("user", "Q1")
        api._save_chat_message("assistant", "A1")
        api._save_chat_message("user", "Q2")
        _close_all(api)

        rows = _read_rows(
            api._db_path,
            "SELECT id, role, content FROM agent.chat_history ORDER BY timestamp",
        )
        assert len(rows) == 3
        ids = [r[0] for r in rows]
        assert len(set(ids)) == 3
        assert rows[0][1] == "user"
        assert rows[1][1] == "assistant"
        assert rows[2][1] == "user"


class TestGetChatHistory:
    def test_empty_history(self, api):
        """get_chat_history returns empty list when no messages."""
        history = api.get_chat_history()
        assert history == []

    def test_returns_persisted_messages(self, api):
        """get_chat_history returns messages saved with _save_chat_message."""
        api._save_chat_message("user", "Hello")
        api._save_chat_message("assistant", "Hi there")
        _close_all(api)

        history = api.get_chat_history()
        assert len(history) == 2
        assert history[0]["role"] == "user"
        assert history[0]["content"] == "Hello"
        assert history[1]["role"] == "assistant"
        assert history[1]["content"] == "Hi there"
        assert "timestamp" in history[0]

    def test_history_ordered_by_timestamp(self, api):
        """Messages are returned in chronological order."""
        api._save_chat_message("user", "First")
        api._save_chat_message("assistant", "Second")
        api._save_chat_message("user", "Third")
        _close_all(api)

        history = api.get_chat_history()
        contents = [m["content"] for m in history]
        assert contents == ["First", "Second", "Third"]


class TestClearChatHistory:
    def test_clear_removes_all_messages(self, api):
        """clear_chat_history deletes all messages."""
        api._save_chat_message("user", "Test 1")
        api._save_chat_message("assistant", "Answer 1")

        result = api.clear_chat_history()
        assert result["status"] == "ok"
        _close_all(api)

        history = api.get_chat_history()
        assert history == []

    def test_clear_empty_history(self, api):
        """Clearing an already empty history succeeds."""
        result = api.clear_chat_history()
        assert result["status"] == "ok"


class TestBuildMessageHistory:
    def test_with_no_prior_history(self, api):
        """With no history, returns just the current message."""
        messages = api._build_message_history("Current question")
        assert len(messages) == 1
        assert messages[0]["role"] == "user"
        assert messages[0]["content"] == "Current question"

    def test_with_prior_messages(self, api):
        """Includes prior messages for multi-turn context."""
        api._save_chat_message("user", "Q1")
        api._save_chat_message("assistant", "A1")
        api._save_chat_message("user", "Q2")
        _close_all(api)

        messages = api._build_message_history("Current question")
        assert len(messages) >= 2
        assert messages[-1]["role"] == "user"
        assert messages[-1]["content"] == "Current question"

    def test_current_message_is_last(self, api):
        """The current user message is always the last in the list."""
        api._save_chat_message("user", "Old question")
        api._save_chat_message("assistant", "Old answer")
        _close_all(api)

        messages = api._build_message_history("New question here")
        assert messages[-1]["content"] == "New question here"
        assert messages[-1]["role"] == "user"


class TestChatWithMockedAPI:
    def test_chat_without_api_key(self, api, monkeypatch):
        """chat() returns error when API key is missing."""
        monkeypatch.setattr("health_platform.utils.keychain.get_secret", lambda _: None)
        result = api.chat("How did I sleep?")
        assert "ANTHROPIC_API_KEY" in result

    def test_chat_saves_user_message(self, api, monkeypatch):
        """chat() persists the user question even if API call fails."""
        monkeypatch.setattr("health_platform.utils.keychain.get_secret", lambda _: None)
        api.chat("My test question")
        _close_all(api)

        rows = _read_rows(
            api._db_path,
            "SELECT role, content FROM agent.chat_history WHERE role = 'user'",
        )
        assert len(rows) == 1
        assert rows[0][1] == "My test question"

    def test_chat_saves_error_response(self, api, monkeypatch):
        """chat() persists the error message as assistant response."""
        monkeypatch.setattr("health_platform.utils.keychain.get_secret", lambda _: None)
        api.chat("Test question")
        _close_all(api)

        rows = _read_rows(
            api._db_path,
            "SELECT role, content FROM agent.chat_history WHERE role = 'assistant'",
        )
        assert len(rows) == 1
        assert "ANTHROPIC_API_KEY" in rows[0][1]


class TestSessionId:
    def test_session_id_is_set(self, api):
        """Each API instance gets a unique session ID."""
        assert api._session_id is not None
        assert len(api._session_id) == 8

    def test_different_instances_different_ids(self, dev_db):
        """Two API instances get different session IDs."""
        api1 = DesktopAPI(dev_db)
        api2 = DesktopAPI(dev_db)
        assert api1._session_id != api2._session_id
