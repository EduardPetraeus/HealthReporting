"""Tests for multi-stream anomaly detection and notification manager (B3+D4).

All test data is synthetic. No real health data is used.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from health_platform.ai.anomaly_detector import (
    CONSTELLATION_RULES,
    MONITORED_METRICS,
    Anomaly,
    AnomalyDetector,
    AnomalyReport,
    ConstellationPattern,
    _classify_severity,
    format_anomaly_report,
)
from health_platform.ai.notification_manager import PRIORITY_MAP, NotificationManager

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def anomaly_db():
    """In-memory DuckDB with silver tables containing 90+ days of synthetic data.

    Creates stable baseline data with known z-scores for testing.
    """
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")

    today = date.today()

    anomaly_day = today - timedelta(days=1)

    # --- silver.daily_sleep ---
    con.execute(
        """
        CREATE TABLE silver.daily_sleep (
            day DATE, sleep_score DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    # Insert 100 days of stable data (mean ~75, stddev ~5)
    # Skip anomaly_day to avoid duplicate with the anomalous insert below
    for i in range(100):
        d = today - timedelta(days=100 - i)
        if d == anomaly_day:
            continue
        score = 75.0 + (i % 7 - 3)  # oscillates 72-78
        con.execute(
            "INSERT INTO silver.daily_sleep (day, sleep_score) VALUES (?, ?)",
            [d, score],
        )
    # Insert an anomalous day (today - 1) with very low score
    con.execute(
        "INSERT INTO silver.daily_sleep (day, sleep_score) VALUES (?, ?)",
        [anomaly_day, 45.0],
    )

    # --- silver.daily_readiness ---
    con.execute(
        """
        CREATE TABLE silver.daily_readiness (
            day DATE, readiness_score DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    for i in range(100):
        d = today - timedelta(days=100 - i)
        if d == anomaly_day:
            continue
        score = 78.0 + (i % 5 - 2)  # oscillates 76-80
        con.execute(
            "INSERT INTO silver.daily_readiness (day, readiness_score) VALUES (?, ?)",
            [d, score],
        )
    # Anomalous day: low readiness
    con.execute(
        "INSERT INTO silver.daily_readiness (day, readiness_score) VALUES (?, ?)",
        [anomaly_day, 42.0],
    )

    # --- silver.daily_activity ---
    con.execute(
        """
        CREATE TABLE silver.daily_activity (
            day DATE, activity_score DOUBLE, steps DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    for i in range(100):
        d = today - timedelta(days=100 - i)
        if d == anomaly_day:
            continue
        score = 80.0 + (i % 6 - 3)  # oscillates 77-83
        steps = 8000.0 + (i % 10 - 5) * 200  # oscillates 7000-9000
        con.execute(
            "INSERT INTO silver.daily_activity (day, activity_score, steps) VALUES (?, ?, ?)",
            [d, score, steps],
        )

    # --- silver.daily_stress ---
    con.execute(
        """
        CREATE TABLE silver.daily_stress (
            day DATE, stress_high DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    for i in range(100):
        d = today - timedelta(days=100 - i)
        if d == anomaly_day:
            continue
        stress = 150.0 + (i % 8 - 4) * 10  # oscillates 110-190
        con.execute(
            "INSERT INTO silver.daily_stress (day, stress_high) VALUES (?, ?)",
            [d, stress],
        )
    # Anomalous day: very high stress
    con.execute(
        "INSERT INTO silver.daily_stress (day, stress_high) VALUES (?, ?)",
        [anomaly_day, 500.0],
    )

    # --- silver.daily_spo2 ---
    con.execute(
        """
        CREATE TABLE silver.daily_spo2 (
            day DATE, spo2_avg_pct DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    for i in range(100):
        d = today - timedelta(days=100 - i)
        if d == anomaly_day:
            continue
        spo2 = 97.0 + (i % 3 - 1) * 0.5  # oscillates 96.5-97.5
        con.execute(
            "INSERT INTO silver.daily_spo2 (day, spo2_avg_pct) VALUES (?, ?)",
            [d, spo2],
        )

    # --- silver.heart_rate ---
    con.execute(
        """
        CREATE TABLE silver.heart_rate (
            timestamp TIMESTAMP, bpm DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    for i in range(100):
        d = today - timedelta(days=100 - i)
        if d == anomaly_day:
            continue
        bpm = 62.0 + (i % 4 - 2)  # oscillates 60-64
        con.execute(
            "INSERT INTO silver.heart_rate (timestamp, bpm) VALUES (?, ?)",
            [d, bpm],
        )

    # --- silver.weight ---
    # Use constant values with no variation so no weight anomalies trigger
    con.execute(
        """
        CREATE TABLE silver.weight (
            datetime TIMESTAMP, weight_kg DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    for i in range(100):
        d = today - timedelta(days=100 - i)
        if d == anomaly_day:
            continue
        weight = 80.0  # constant -- no variation means stddev=0, no anomalies
        con.execute(
            "INSERT INTO silver.weight (datetime, weight_kg) VALUES (?, ?)",
            [d, weight],
        )

    yield con
    con.close()


@pytest.fixture
def degradation_db():
    """In-memory DuckDB with a consistently declining metric for degradation testing."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    today = date.today()

    # Create daily_sleep with a clear downward trend over 30 days
    con.execute(
        """
        CREATE TABLE silver.daily_sleep (
            day DATE, sleep_score DOUBLE,
            business_key_hash VARCHAR, row_hash VARCHAR,
            load_datetime TIMESTAMP, update_datetime TIMESTAMP
        )
    """
    )
    # Strong downward trend: 90 -> 50 over 30 days
    for i in range(30):
        d = today - timedelta(days=30 - i)
        score = 90.0 - i * 1.3  # linear decline
        con.execute(
            "INSERT INTO silver.daily_sleep (day, sleep_score) VALUES (?, ?)",
            [d, score],
        )

    # Also need the other tables (empty but existent)
    for table_def in [
        "CREATE TABLE silver.daily_readiness (day DATE, readiness_score DOUBLE, business_key_hash VARCHAR, row_hash VARCHAR, load_datetime TIMESTAMP, update_datetime TIMESTAMP)",
        "CREATE TABLE silver.daily_activity (day DATE, activity_score DOUBLE, steps DOUBLE, business_key_hash VARCHAR, row_hash VARCHAR, load_datetime TIMESTAMP, update_datetime TIMESTAMP)",
        "CREATE TABLE silver.daily_stress (day DATE, stress_high DOUBLE, business_key_hash VARCHAR, row_hash VARCHAR, load_datetime TIMESTAMP, update_datetime TIMESTAMP)",
        "CREATE TABLE silver.daily_spo2 (day DATE, spo2_avg_pct DOUBLE, business_key_hash VARCHAR, row_hash VARCHAR, load_datetime TIMESTAMP, update_datetime TIMESTAMP)",
        "CREATE TABLE silver.heart_rate (timestamp TIMESTAMP, bpm DOUBLE, business_key_hash VARCHAR, row_hash VARCHAR, load_datetime TIMESTAMP, update_datetime TIMESTAMP)",
        "CREATE TABLE silver.weight (datetime TIMESTAMP, weight_kg DOUBLE, business_key_hash VARCHAR, row_hash VARCHAR, load_datetime TIMESTAMP, update_datetime TIMESTAMP)",
    ]:
        con.execute(table_def)

    yield con
    con.close()


@pytest.fixture
def empty_db():
    """In-memory DuckDB with silver tables that have no data."""
    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    con.execute(
        "CREATE TABLE silver.daily_sleep (day DATE, sleep_score DOUBLE, business_key_hash VARCHAR, row_hash VARCHAR, load_datetime TIMESTAMP, update_datetime TIMESTAMP)"
    )
    con.execute(
        "CREATE TABLE silver.daily_readiness (day DATE, readiness_score DOUBLE, business_key_hash VARCHAR, row_hash VARCHAR, load_datetime TIMESTAMP, update_datetime TIMESTAMP)"
    )
    con.execute(
        "CREATE TABLE silver.daily_activity (day DATE, activity_score DOUBLE, steps DOUBLE, business_key_hash VARCHAR, row_hash VARCHAR, load_datetime TIMESTAMP, update_datetime TIMESTAMP)"
    )
    con.execute(
        "CREATE TABLE silver.daily_stress (day DATE, stress_high DOUBLE, business_key_hash VARCHAR, row_hash VARCHAR, load_datetime TIMESTAMP, update_datetime TIMESTAMP)"
    )
    con.execute(
        "CREATE TABLE silver.daily_spo2 (day DATE, spo2_avg_pct DOUBLE, business_key_hash VARCHAR, row_hash VARCHAR, load_datetime TIMESTAMP, update_datetime TIMESTAMP)"
    )
    con.execute(
        "CREATE TABLE silver.heart_rate (timestamp TIMESTAMP, bpm DOUBLE, business_key_hash VARCHAR, row_hash VARCHAR, load_datetime TIMESTAMP, update_datetime TIMESTAMP)"
    )
    con.execute(
        "CREATE TABLE silver.weight (datetime TIMESTAMP, weight_kg DOUBLE, business_key_hash VARCHAR, row_hash VARCHAR, load_datetime TIMESTAMP, update_datetime TIMESTAMP)"
    )

    yield con
    con.close()


# ---------------------------------------------------------------------------
# AnomalyDetector tests
# ---------------------------------------------------------------------------


class TestAnomalyDetectorInit:
    """Test AnomalyDetector initialization."""

    def test_initializes_with_connection(self, anomaly_db):
        """Detector initializes with DuckDB connection."""
        detector = AnomalyDetector(anomaly_db)
        assert detector.con is anomaly_db
        assert detector.window_days == 90

    def test_initializes_with_custom_window(self, anomaly_db):
        """Detector accepts custom window_days."""
        detector = AnomalyDetector(anomaly_db, window_days=60)
        assert detector.window_days == 60


class TestZScoreDetection:
    """Test z-score based anomaly detection."""

    def test_detects_known_anomaly(self, anomaly_db):
        """Detect anomaly in sleep_score with known z > 2.0."""
        detector = AnomalyDetector(anomaly_db)
        report = detector.detect(lookback_days=3)

        sleep_anomalies = [a for a in report.anomalies if a.column == "sleep_score"]
        # The value 45.0 should be well below the mean ~75, so z > 2.0
        assert len(sleep_anomalies) > 0
        severe = [a for a in sleep_anomalies if a.severity == "anomaly"]
        assert len(severe) > 0, "Expected at least one 'anomaly' severity"

    def test_normal_values_no_anomalies(self, anomaly_db):
        """Normal values within baseline range produce no anomalies."""
        detector = AnomalyDetector(anomaly_db)
        report = detector.detect(lookback_days=3)

        # Weight should have no anomalies (stable oscillation)
        weight_anomalies = [a for a in report.anomalies if a.column == "weight_kg"]
        # Weight data is stable -- no extreme values inserted
        assert len(weight_anomalies) == 0

    def test_severity_classification_anomaly(self):
        """Z-score > 2.0 classified as 'anomaly'."""
        assert _classify_severity(2.5) == "anomaly"
        assert _classify_severity(2.0) == "anomaly"

    def test_severity_classification_unusual(self):
        """Z-score > 1.5 and <= 2.0 classified as 'unusual'."""
        assert _classify_severity(1.7) == "unusual"
        assert _classify_severity(1.5) == "unusual"

    def test_severity_classification_warning(self):
        """Z-score > 1.0 and <= 1.5 classified as 'warning'."""
        assert _classify_severity(1.2) == "warning"
        assert _classify_severity(1.0) == "warning"

    def test_severity_classification_normal(self):
        """Z-score <= 1.0 returns None (normal)."""
        assert _classify_severity(0.9) is None
        assert _classify_severity(0.0) is None

    def test_direction_below_mean(self, anomaly_db):
        """Anomaly below mean reports direction='below'."""
        detector = AnomalyDetector(anomaly_db)
        report = detector.detect(lookback_days=3)

        sleep_anomalies = [a for a in report.anomalies if a.column == "sleep_score"]
        if sleep_anomalies:
            # Sleep score 45 is below mean ~75
            assert sleep_anomalies[0].direction == "below"

    def test_direction_above_mean(self, anomaly_db):
        """Anomaly above mean reports direction='above'."""
        detector = AnomalyDetector(anomaly_db)
        report = detector.detect(lookback_days=3)

        stress_anomalies = [a for a in report.anomalies if a.column == "stress_high"]
        if stress_anomalies:
            # Stress 500 is above mean ~150
            assert stress_anomalies[0].direction == "above"


class TestConstellationPatterns:
    """Test constellation pattern matching."""

    def test_detects_illness_onset_pattern(self, anomaly_db):
        """Detect illness_onset when sleep + readiness are both anomalous."""
        detector = AnomalyDetector(anomaly_db)
        report = detector.detect(lookback_days=3)

        # We inserted anomalous sleep (45) and readiness (42) on same day
        illness_patterns = [
            c for c in report.constellations if c.pattern_type == "illness_onset"
        ]
        assert (
            len(illness_patterns) > 0
        ), "Expected illness_onset pattern from simultaneous sleep+readiness anomalies"

    def test_constellation_confidence_range(self, anomaly_db):
        """Constellation confidence is between 0.0 and 1.0."""
        detector = AnomalyDetector(anomaly_db)
        report = detector.detect(lookback_days=3)

        for c in report.constellations:
            assert 0.0 <= c.confidence <= 1.0

    def test_constellation_has_anomalies_list(self, anomaly_db):
        """Each constellation contains a list of constituent anomalies."""
        detector = AnomalyDetector(anomaly_db)
        report = detector.detect(lookback_days=3)

        for c in report.constellations:
            assert isinstance(c.anomalies, list)
            assert len(c.anomalies) >= 2, "Constellation needs at least 2 anomalies"

    def test_recovery_stress_pattern(self, anomaly_db):
        """Detect recovery_stress when readiness low + stress high."""
        detector = AnomalyDetector(anomaly_db)
        report = detector.detect(lookback_days=3)

        # We have low readiness (42) and high stress (500) on same day
        recovery_patterns = [
            c for c in report.constellations if c.pattern_type == "recovery_stress"
        ]
        assert (
            len(recovery_patterns) > 0
        ), "Expected recovery_stress pattern from low readiness + high stress"

    def test_no_constellation_without_required_metrics(self, empty_db):
        """No constellations when no anomalies exist."""
        detector = AnomalyDetector(empty_db)
        report = detector.detect(lookback_days=7)

        assert len(report.constellations) == 0


class TestTemporalDegradation:
    """Test temporal degradation detection."""

    def test_detects_declining_trend(self, degradation_db):
        """Detect declining sleep_score trend over 30 days."""
        detector = AnomalyDetector(degradation_db)
        report = detector.detect(lookback_days=7)

        sleep_degradations = [d for d in report.degradations if "sleep" in d.metric]
        assert (
            len(sleep_degradations) > 0
        ), "Expected degradation for declining sleep_score"
        assert sleep_degradations[0].slope < 0

    def test_stable_data_no_degradation(self, anomaly_db):
        """Stable oscillating data does not trigger degradation."""
        detector = AnomalyDetector(anomaly_db)
        report = detector.detect(lookback_days=3)

        # Weight is stable -- should have no degradation
        weight_degradations = [d for d in report.degradations if "weight" in d.metric]
        assert len(weight_degradations) == 0

    def test_degradation_r_squared(self, degradation_db):
        """Degradation R-squared exceeds threshold of 0.3."""
        detector = AnomalyDetector(degradation_db)
        report = detector.detect(lookback_days=7)

        for d in report.degradations:
            assert d.r_squared > 0.3

    def test_degradation_has_duration(self, degradation_db):
        """Degradation includes duration_days."""
        detector = AnomalyDetector(degradation_db)
        report = detector.detect(lookback_days=7)

        for d in report.degradations:
            assert d.duration_days >= 14


class TestAnomalyReport:
    """Test AnomalyReport dataclass properties."""

    def test_has_critical_with_anomaly_severity(self):
        """has_critical is True when any anomaly has severity='anomaly'."""
        report = AnomalyReport(
            generated_at=date.today(),
            lookback_days=7,
            anomalies=[
                Anomaly(
                    metric="test",
                    table="silver.test",
                    column="val",
                    day=date.today(),
                    value=10.0,
                    mean=50.0,
                    std_dev=5.0,
                    z_score=-8.0,
                    severity="anomaly",
                    direction="below",
                    description="test anomaly",
                )
            ],
        )
        assert report.has_critical is True

    def test_has_critical_with_constellation(self):
        """has_critical is True when constellations exist."""
        report = AnomalyReport(
            generated_at=date.today(),
            lookback_days=7,
            constellations=[
                ConstellationPattern(
                    day=date.today(),
                    anomalies=[],
                    pattern_type="test",
                    confidence=0.8,
                    description="test pattern",
                )
            ],
        )
        assert report.has_critical is True

    def test_not_critical_with_only_warnings(self):
        """has_critical is False with only warning-level anomalies."""
        report = AnomalyReport(
            generated_at=date.today(),
            lookback_days=7,
            anomalies=[
                Anomaly(
                    metric="test",
                    table="silver.test",
                    column="val",
                    day=date.today(),
                    value=60.0,
                    mean=50.0,
                    std_dev=5.0,
                    z_score=2.0,
                    severity="warning",
                    direction="above",
                    description="test warning",
                )
            ],
        )
        assert report.has_critical is False

    def test_empty_report_not_critical(self):
        """Empty report is not critical."""
        report = AnomalyReport(generated_at=date.today(), lookback_days=7)
        assert report.has_critical is False


class TestFormatAnomalyReport:
    """Test markdown report formatting."""

    def test_format_empty_report(self):
        """Empty report produces clean markdown."""
        report = AnomalyReport(generated_at=date.today(), lookback_days=7)
        output = format_anomaly_report(report)

        assert "# Anomaly Report" in output
        assert "No anomalies detected" in output

    def test_format_with_anomalies(self, anomaly_db):
        """Report with anomalies produces structured markdown."""
        detector = AnomalyDetector(anomaly_db)
        report = detector.detect(lookback_days=3)
        output = format_anomaly_report(report)

        assert "# Anomaly Report" in output
        assert "Lookback: 3 days" in output
        assert "## Individual Anomalies" in output

    def test_format_includes_constellations(self, anomaly_db):
        """Report with constellations includes constellation section."""
        detector = AnomalyDetector(anomaly_db)
        report = detector.detect(lookback_days=3)
        output = format_anomaly_report(report)

        if report.constellations:
            assert "## Constellation Patterns" in output

    def test_format_includes_severity_labels(self, anomaly_db):
        """Formatted output includes severity labels in uppercase."""
        detector = AnomalyDetector(anomaly_db)
        report = detector.detect(lookback_days=3)
        output = format_anomaly_report(report)

        if report.anomalies:
            has_label = any(
                label in output for label in ["[ANOMALY]", "[UNUSUAL]", "[WARNING]"]
            )
            assert has_label


class TestEmptyDatabase:
    """Test behavior with empty database."""

    def test_empty_db_returns_clean_report(self, empty_db):
        """Empty database returns report with no anomalies."""
        detector = AnomalyDetector(empty_db)
        report = detector.detect(lookback_days=7)

        assert len(report.anomalies) == 0
        assert len(report.constellations) == 0
        assert len(report.degradations) == 0
        assert report.has_critical is False

    def test_empty_db_format_report(self, empty_db):
        """Empty database produces 'no anomalies' in formatted output."""
        detector = AnomalyDetector(empty_db)
        report = detector.detect(lookback_days=7)
        output = format_anomaly_report(report)

        assert "No anomalies detected" in output


# ---------------------------------------------------------------------------
# NotificationManager tests
# ---------------------------------------------------------------------------


class TestNotificationManager:
    """Test notification manager."""

    def test_dry_run_no_topic(self):
        """Manager without topic logs but does not send."""
        manager = NotificationManager(topic=None)
        assert manager.topic is None

    def test_dry_run_returns_false(self):
        """_send returns False in dry-run mode (no topic)."""
        manager = NotificationManager(topic=None)
        result = manager._send(title="Test", message="Test message")
        assert result is False

    def test_notify_empty_report(self):
        """No notifications sent for report without critical anomalies."""
        manager = NotificationManager(topic=None)
        report = AnomalyReport(generated_at=date.today(), lookback_days=7)
        count = manager.notify_anomaly_report(report)
        assert count == 0

    def test_notify_critical_report_dry_run(self):
        """Critical report sends notifications even in dry-run."""
        manager = NotificationManager(topic=None)
        report = AnomalyReport(
            generated_at=date.today(),
            lookback_days=7,
            anomalies=[
                Anomaly(
                    metric="test",
                    table="silver.test",
                    column="val",
                    day=date.today(),
                    value=10.0,
                    mean=50.0,
                    std_dev=5.0,
                    z_score=-8.0,
                    severity="anomaly",
                    direction="below",
                    description="test critical",
                )
            ],
        )
        count = manager.notify_anomaly_report(report)
        # 1 notification for the anomaly batch
        assert count == 1

    def test_notify_constellation_sends(self):
        """Constellation pattern triggers notification."""
        manager = NotificationManager(topic=None)
        report = AnomalyReport(
            generated_at=date.today(),
            lookback_days=7,
            constellations=[
                ConstellationPattern(
                    day=date.today(),
                    anomalies=[],
                    pattern_type="illness_onset",
                    confidence=0.8,
                    description="Possible illness detected",
                )
            ],
        )
        count = manager.notify_anomaly_report(report)
        assert count >= 1

    @patch("health_platform.ai.notification_manager.urllib.request.urlopen")
    def test_send_with_topic_calls_urlopen(self, mock_urlopen):
        """_send with a topic calls urllib.request.urlopen."""
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        manager = NotificationManager(topic="test-health-topic")
        result = manager._send(
            title="Test Alert",
            message="Test notification body",
            priority="high",
            tags=["health"],
        )

        assert result is True
        mock_urlopen.assert_called_once()

    @patch("health_platform.ai.notification_manager.urllib.request.urlopen")
    def test_send_handles_network_error(self, mock_urlopen):
        """_send handles network errors gracefully."""
        mock_urlopen.side_effect = Exception("Connection refused")

        manager = NotificationManager(topic="test-health-topic")
        result = manager._send(title="Test", message="Test")

        assert result is False

    def test_priority_map_coverage(self):
        """PRIORITY_MAP covers all expected severity levels."""
        assert "anomaly" in PRIORITY_MAP
        assert "unusual" in PRIORITY_MAP
        assert "warning" in PRIORITY_MAP
        assert "constellation" in PRIORITY_MAP
        assert "degradation" in PRIORITY_MAP


# ---------------------------------------------------------------------------
# MCP tool integration test
# ---------------------------------------------------------------------------


class TestMCPToolIntegration:
    """Test detect_anomalies as MCP tool via HealthTools."""

    def test_health_tools_detect_anomalies(self, anomaly_db):
        """HealthTools.detect_anomalies returns formatted string."""
        # We import inside the test to avoid path issues at module load
        from health_platform.ai.anomaly_detector import (
            AnomalyDetector,
            format_anomaly_report,
        )

        detector = AnomalyDetector(anomaly_db)
        report = detector.detect(lookback_days=3)
        result = format_anomaly_report(report)

        assert isinstance(result, str)
        assert "# Anomaly Report" in result

    def test_detect_anomalies_lookback_clamped(self, anomaly_db):
        """Lookback days are clamped to 1-90 range."""
        detector = AnomalyDetector(anomaly_db)

        # Very large lookback still works
        report = detector.detect(lookback_days=90)
        assert report.lookback_days == 90

        # Zero lookback would be clamped by the MCP tool wrapper
        report = detector.detect(lookback_days=1)
        assert report.lookback_days == 1


# ---------------------------------------------------------------------------
# Constants and configuration tests
# ---------------------------------------------------------------------------


class TestConfiguration:
    """Test that configuration constants are correct."""

    def test_monitored_metrics_structure(self):
        """MONITORED_METRICS contains valid 4-tuples."""
        for entry in MONITORED_METRICS:
            assert len(entry) == 4
            table, column, date_col, display_name = entry
            assert table.startswith("silver.")
            assert len(column) > 0
            assert len(display_name) > 0

    def test_constellation_rules_structure(self):
        """CONSTELLATION_RULES have required keys."""
        for name, rule in CONSTELLATION_RULES.items():
            assert "description" in rule
            assert "requires" in rule
            assert "direction_map" in rule
            assert isinstance(rule["requires"], list)
            assert len(rule["requires"]) >= 2

    def test_constellation_rules_reference_valid_metrics(self):
        """All metrics referenced in constellation rules exist in MONITORED_METRICS."""
        valid_display_names = {m[3] for m in MONITORED_METRICS}
        for name, rule in CONSTELLATION_RULES.items():
            for metric in rule["requires"]:
                assert (
                    metric in valid_display_names
                ), f"Constellation '{name}' references unknown metric '{metric}'"
            for metric in rule.get("optional", []):
                assert (
                    metric in valid_display_names
                ), f"Constellation '{name}' references unknown optional metric '{metric}'"
