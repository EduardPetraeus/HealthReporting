"""DesktopAPI — Python methods exposed to JavaScript via pywebview.

All public methods are callable from the frontend as:
    window.pywebview.api.method_name(args)

Returns JSON-serializable dicts/lists for JavaScript consumption.
"""

from __future__ import annotations

import json
import threading
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import duckdb

from health_platform.mcp.health_tools import HealthTools
from health_platform.mcp.query_builder import QueryBuilder
from health_platform.utils.logging_config import get_logger

logger = get_logger("desktop.api")

_CONTRACTS_DIR = Path(__file__).resolve().parents[1] / "contracts" / "metrics"


def _sanitize(value: Any) -> Any:
    """Convert DuckDB types to JSON-serializable Python types."""
    import decimal

    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _sanitize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize(v) for v in value]
    return value


class DesktopAPI:
    """API exposed to the pywebview JavaScript bridge.

    All public methods return JSON-serializable values.
    The DuckDB connection uses read_only=True for queries.
    A separate write connection (with threading lock) persists chat history.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._window = None
        self._con: Optional[duckdb.DuckDBPyConnection] = None
        self._write_con: Optional[duckdb.DuckDBPyConnection] = None
        self._write_lock = threading.Lock()
        self._tools: Optional[HealthTools] = None
        self._query_builder = QueryBuilder(_CONTRACTS_DIR)
        self._session_id = str(uuid.uuid4())[:8]

    def set_window(self, window) -> None:
        self._window = window

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create a read-only DuckDB connection."""
        if self._con is None:
            if not Path(self._db_path).exists():
                raise FileNotFoundError(f"Database not found: {self._db_path}")
            self._con = duckdb.connect(self._db_path, read_only=True)
        return self._con

    def _get_write_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create a writable DuckDB connection for chat history."""
        if self._write_con is None:
            self._write_con = duckdb.connect(self._db_path)
            self._ensure_chat_history_table()
        return self._write_con

    def _ensure_chat_history_table(self) -> None:
        """Create agent.chat_history table if it does not exist."""
        con = self._write_con
        if con is None:
            return
        con.execute("CREATE SCHEMA IF NOT EXISTS agent")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS agent.chat_history (
                id VARCHAR PRIMARY KEY,
                session_id VARCHAR,
                role VARCHAR NOT NULL,
                content TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _get_tools(self) -> HealthTools:
        """Get or create a HealthTools instance."""
        if self._tools is None:
            self._tools = HealthTools(self._get_connection())
        return self._tools

    # ------------------------------------------------------------------
    # Chat
    # ------------------------------------------------------------------

    def chat(self, question: str) -> str:
        """Send a health question to Claude and stream the response.

        Gathers health data context, builds multi-turn message history,
        streams from Claude API, and pushes chunks to the frontend via
        evaluate_js(). Persists both user question and AI response.
        """
        import anthropic

        from health_platform.api.chat_engine import SYSTEM_PROMPT, _gather_context
        from health_platform.utils.keychain import get_secret

        self._save_chat_message("user", question)

        api_key = get_secret("ANTHROPIC_API_KEY")
        if not api_key:
            error_msg = "ANTHROPIC_API_KEY not found in keychain."
            self._save_chat_message("assistant", error_msg)
            return error_msg

        try:
            tools = self._get_tools()
            context = _gather_context(tools, question)
        except Exception as exc:
            logger.debug("Failed to gather context: %s", exc)
            context = "No health data available."

        safe_question = question.replace("---", "").strip()
        user_message = (
            f"<health_data>\n{context}\n</health_data>\n\n"
            f"<user_question>\n{safe_question}\n</user_question>"
        )

        messages = self._build_message_history(user_message)

        client = anthropic.Anthropic(api_key=api_key)
        full_response = ""

        try:
            with client.messages.stream(
                model="claude-sonnet-4-20250514",
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                messages=messages,
            ) as stream:
                for text in stream.text_stream:
                    full_response += text
                    if self._window:
                        escaped = json.dumps(text)
                        self._window.evaluate_js(f"appendStreamChunk({escaped})")
        except Exception as exc:
            logger.error("Claude API error: %s", exc)
            if not full_response:
                full_response = f"Error: {exc}"

        if self._window:
            self._window.evaluate_js("finishStream()")

        self._save_chat_message("assistant", full_response)
        return full_response

    def _build_message_history(self, current_user_message: str) -> list[dict]:
        """Build message list with recent chat history for multi-turn context."""
        messages = []
        try:
            con = self._get_connection()
            rows = con.execute(
                """
                SELECT role, content FROM agent.chat_history
                ORDER BY timestamp DESC
                LIMIT 20
                """
            ).fetchall()
            # Reverse to chronological order, skip the last user message (it's current)
            for role, content in reversed(rows[1:] if rows else []):
                messages.append({"role": role, "content": content})
        except Exception:
            pass

        messages.append({"role": "user", "content": current_user_message})
        return messages

    def get_chat_history(self) -> list[dict]:
        """Return all chat messages for the current session display."""
        try:
            con = self._get_connection()
            rows = con.execute(
                """
                SELECT role, content, timestamp
                FROM agent.chat_history
                ORDER BY timestamp ASC
                """
            ).fetchall()
            return [
                {"role": r[0], "content": r[1], "timestamp": str(r[2])} for r in rows
            ]
        except Exception:
            return []

    def clear_chat_history(self) -> dict:
        """Delete all chat history."""
        try:
            with self._write_lock:
                con = self._get_write_connection()
                con.execute("DELETE FROM agent.chat_history")
            return {"status": "ok"}
        except Exception as exc:
            logger.debug("Failed to clear chat history: %s", exc)
            return {"status": "error", "message": str(exc)}

    def _save_chat_message(self, role: str, content: str) -> None:
        """Persist a chat message to DuckDB."""
        try:
            with self._write_lock:
                con = self._get_write_connection()
                con.execute(
                    "INSERT INTO agent.chat_history (id, session_id, role, content, timestamp) VALUES (?, ?, ?, ?, ?)",
                    [
                        str(uuid.uuid4()),
                        self._session_id,
                        role,
                        content,
                        datetime.now().isoformat(),
                    ],
                )
        except Exception as exc:
            logger.debug("Failed to save chat message: %s", exc)

    # ------------------------------------------------------------------
    # Dashboard
    # ------------------------------------------------------------------

    def get_dashboard_data(self) -> dict[str, Any]:
        """Return all dashboard data in a single call.

        Returns a dict with:
        - health_score: today's composite score + status
        - kpis: latest values for sleep, readiness, steps, resting HR
        - sparklines: 7-day data series for each KPI
        - trends: 30-day sleep + readiness overlay data
        - alerts: any active alerts/anomalies
        """
        try:
            con = self._get_connection()
        except FileNotFoundError:
            return {
                "error": "no_database",
                "message": f"Database not found: {self._db_path}",
            }

        today = date.today()
        start_7d = (today - timedelta(days=7)).isoformat()
        start_30d = (today - timedelta(days=30)).isoformat()
        end = today.isoformat()

        result: dict[str, Any] = {
            "timestamp": today.isoformat(),
            "health_score": self._get_health_score(con, end),
            "kpis": self._get_kpis(con, end),
            "sparklines": self._get_sparklines(con, start_7d, end),
            "trends": self._get_trends(con, start_30d, end),
            "alerts": self._get_alerts(con, start_7d, end),
        }
        return _sanitize(result)

    def _get_health_score(self, con: duckdb.DuckDBPyConnection, end: str) -> dict:
        """Compute composite health score for the latest day with data."""
        try:
            row = con.execute(
                """
                SELECT
                    COALESCE(s.day, r.day, a.day) AS day,
                    s.sleep_score,
                    r.readiness_score,
                    a.activity_score,
                    ROUND(
                        (COALESCE(s.sleep_score, 0) * 0.35 +
                         COALESCE(r.readiness_score, 0) * 0.35 +
                         COALESCE(a.activity_score, 0) * 0.30), 1
                    ) AS health_score
                FROM silver.daily_sleep s
                FULL OUTER JOIN silver.daily_readiness r ON s.day = r.day
                FULL OUTER JOIN silver.daily_activity a ON COALESCE(s.day, r.day) = a.day
                WHERE COALESCE(s.day, r.day, a.day) <= ?
                ORDER BY COALESCE(s.day, r.day, a.day) DESC
                LIMIT 1
                """,
                [end],
            ).fetchone()
        except Exception as exc:
            logger.debug("Health score query failed: %s", exc)
            return {"score": None, "status": "unknown", "day": None}

        if not row:
            return {"score": None, "status": "unknown", "day": None}

        score = row[4]
        if score is None:
            status = "unknown"
        elif score >= 85:
            status = "excellent"
        elif score >= 70:
            status = "good"
        elif score >= 55:
            status = "fair"
        else:
            status = "poor"

        return {
            "score": score,
            "status": status,
            "day": str(row[0]),
            "components": {
                "sleep": row[1],
                "readiness": row[2],
                "activity": row[3],
            },
        }

    def _get_kpis(self, con: duckdb.DuckDBPyConnection, end: str) -> dict:
        """Get latest values for the 4 main KPIs."""
        kpis = {}

        # Sleep Score
        kpis["sleep_score"] = self._latest_value(
            con,
            "SELECT day, sleep_score FROM silver.daily_sleep WHERE day <= ? ORDER BY day DESC LIMIT 1",
            end,
            "Sleep Score",
            "score",
        )

        # Readiness Score
        kpis["readiness_score"] = self._latest_value(
            con,
            "SELECT day, readiness_score FROM silver.daily_readiness WHERE day <= ? ORDER BY day DESC LIMIT 1",
            end,
            "Readiness",
            "score",
        )

        # Steps
        kpis["steps"] = self._latest_value(
            con,
            "SELECT day, steps FROM silver.daily_activity WHERE day <= ? ORDER BY day DESC LIMIT 1",
            end,
            "Steps",
            "steps",
        )

        # Resting Heart Rate
        kpis["resting_hr"] = self._latest_hr(con, end)

        return kpis

    def _latest_value(
        self,
        con: duckdb.DuckDBPyConnection,
        sql: str,
        end: str,
        label: str,
        unit: str,
    ) -> dict:
        """Execute a latest-value query and return structured result."""
        try:
            row = con.execute(sql, [end]).fetchone()
        except Exception as exc:
            logger.debug("KPI query failed for %s: %s", label, exc)
            return {"label": label, "value": None, "day": None, "unit": unit}

        if not row:
            return {"label": label, "value": None, "day": None, "unit": unit}

        return {
            "label": label,
            "value": row[1],
            "day": str(row[0]),
            "unit": unit,
        }

    def _latest_hr(self, con: duckdb.DuckDBPyConnection, end: str) -> dict:
        """Get latest resting heart rate (min bpm per day)."""
        try:
            row = con.execute(
                """
                SELECT DATE(timestamp) AS day, MIN(bpm) AS resting_hr
                FROM silver.heart_rate
                WHERE DATE(timestamp) <= ?
                GROUP BY DATE(timestamp)
                ORDER BY day DESC
                LIMIT 1
                """,
                [end],
            ).fetchone()
        except Exception as exc:
            logger.debug("Resting HR query failed: %s", exc)
            return {"label": "Resting HR", "value": None, "day": None, "unit": "bpm"}

        if not row:
            return {"label": "Resting HR", "value": None, "day": None, "unit": "bpm"}

        return {
            "label": "Resting HR",
            "value": row[1],
            "day": str(row[0]),
            "unit": "bpm",
        }

    def _get_sparklines(
        self, con: duckdb.DuckDBPyConnection, start: str, end: str
    ) -> dict:
        """Get 7-day data series for sparkline charts."""
        sparklines = {}

        # Sleep scores
        sparklines["sleep_score"] = self._series(
            con,
            "SELECT day, sleep_score FROM silver.daily_sleep WHERE day BETWEEN ? AND ? ORDER BY day",
            start,
            end,
        )

        # Readiness scores
        sparklines["readiness_score"] = self._series(
            con,
            "SELECT day, readiness_score FROM silver.daily_readiness WHERE day BETWEEN ? AND ? ORDER BY day",
            start,
            end,
        )

        # Steps
        sparklines["steps"] = self._series(
            con,
            "SELECT day, steps FROM silver.daily_activity WHERE day BETWEEN ? AND ? ORDER BY day",
            start,
            end,
        )

        # Resting HR
        sparklines["resting_hr"] = self._series(
            con,
            """
            SELECT DATE(timestamp) AS day, MIN(bpm) AS resting_hr
            FROM silver.heart_rate
            WHERE DATE(timestamp) BETWEEN ? AND ?
            GROUP BY DATE(timestamp)
            ORDER BY day
            """,
            start,
            end,
        )

        return sparklines

    def _series(
        self,
        con: duckdb.DuckDBPyConnection,
        sql: str,
        start: str,
        end: str,
    ) -> list[dict]:
        """Execute a time series query and return [{day, value}, ...]."""
        try:
            rows = con.execute(sql, [start, end]).fetchall()
        except Exception as exc:
            logger.debug("Series query failed: %s", exc)
            return []

        return [{"day": str(r[0]), "value": r[1]} for r in rows if r[1] is not None]

    def _get_trends(self, con: duckdb.DuckDBPyConnection, start: str, end: str) -> dict:
        """Get 30-day trend data for sleep + readiness overlay chart."""
        try:
            rows = con.execute(
                """
                SELECT
                    COALESCE(s.day, r.day) AS day,
                    s.sleep_score,
                    r.readiness_score
                FROM silver.daily_sleep s
                FULL OUTER JOIN silver.daily_readiness r ON s.day = r.day
                WHERE COALESCE(s.day, r.day) BETWEEN ? AND ?
                ORDER BY COALESCE(s.day, r.day)
                """,
                [start, end],
            ).fetchall()
        except Exception as exc:
            logger.debug("Trends query failed: %s", exc)
            return {"days": [], "sleep": [], "readiness": []}

        days = []
        sleep = []
        readiness = []
        for row in rows:
            days.append(str(row[0]))
            sleep.append(row[1])
            readiness.append(row[2])

        return {"days": days, "sleep": sleep, "readiness": readiness}

    def _get_alerts(
        self, con: duckdb.DuckDBPyConnection, start: str, end: str
    ) -> list[dict]:
        """Check for notable patterns in recent data."""
        alerts = []

        # Low sleep score
        try:
            row = con.execute(
                """
                SELECT day, sleep_score
                FROM silver.daily_sleep
                WHERE day BETWEEN ? AND ? AND sleep_score < 60
                ORDER BY day DESC
                LIMIT 1
                """,
                [start, end],
            ).fetchone()
            if row:
                alerts.append(
                    {
                        "type": "warning",
                        "metric": "sleep_score",
                        "message": f"Low sleep score ({row[1]}) on {row[0]}",
                    }
                )
        except Exception:
            pass

        # Low readiness
        try:
            row = con.execute(
                """
                SELECT day, readiness_score
                FROM silver.daily_readiness
                WHERE day BETWEEN ? AND ? AND readiness_score < 60
                ORDER BY day DESC
                LIMIT 1
                """,
                [start, end],
            ).fetchone()
            if row:
                alerts.append(
                    {
                        "type": "warning",
                        "metric": "readiness_score",
                        "message": f"Low readiness ({row[1]}) on {row[0]}",
                    }
                )
        except Exception:
            pass

        # Elevated resting HR (above 70 bpm)
        try:
            row = con.execute(
                """
                SELECT DATE(timestamp) AS day, MIN(bpm) AS resting_hr
                FROM silver.heart_rate
                WHERE DATE(timestamp) BETWEEN ? AND ?
                GROUP BY DATE(timestamp)
                HAVING MIN(bpm) > 70
                ORDER BY day DESC
                LIMIT 1
                """,
                [start, end],
            ).fetchone()
            if row:
                alerts.append(
                    {
                        "type": "warning",
                        "metric": "resting_hr",
                        "message": f"Elevated resting HR ({row[1]} bpm) on {row[0]}",
                    }
                )
        except Exception:
            pass

        return alerts

    # ------------------------------------------------------------------
    # Reports
    # ------------------------------------------------------------------

    def generate_report(
        self,
        start_date: str,
        end_date: str,
        sections: list[str] | None = None,
    ) -> dict[str, Any]:
        """Generate a PDF report and return base64-encoded PDF.

        Called from the Reports tab in the frontend.
        Returns dict with 'pdf_base64' key on success, 'error' on failure.
        """
        import base64

        from health_platform.desktop.reports.generator import ReportGenerator

        try:
            gen = ReportGenerator(self._db_path)
            pdf_bytes = gen.generate_report(start_date, end_date, sections)
            gen.close()
            pdf_b64 = base64.b64encode(pdf_bytes).decode("ascii")
            return {"pdf_base64": pdf_b64, "error": None}
        except Exception as exc:
            logger.error("Report generation failed: %s", exc)
            return {"pdf_base64": None, "error": str(exc)}

    def download_report(
        self,
        start_date: str,
        end_date: str,
        sections: list[str] | None = None,
    ) -> dict[str, Any]:
        """Generate a PDF report and save it to the Desktop.

        Returns dict with 'path' key on success, 'error' on failure.
        """
        from health_platform.desktop.reports.generator import ReportGenerator

        try:
            gen = ReportGenerator(self._db_path)
            pdf_bytes = gen.generate_report(start_date, end_date, sections)
            gen.close()

            desktop_dir = Path.home() / "Desktop"
            desktop_dir.mkdir(exist_ok=True)
            filename = f"health_report_{start_date}_to_{end_date}.pdf"
            filepath = desktop_dir / filename
            filepath.write_bytes(pdf_bytes)

            logger.info("Report saved to %s", filepath)
            return {"path": str(filepath), "error": None}
        except Exception as exc:
            logger.error("Report download failed: %s", exc)
            return {"path": None, "error": str(exc)}

    # ------------------------------------------------------------------
    # Metric query (for Data Explorer)
    # ------------------------------------------------------------------

    def query_metric(
        self,
        metric: str,
        computation: str = "daily_value",
        date_range: str = "last_7_days",
    ) -> dict[str, Any]:
        """Query a metric via its semantic contract.

        Returns structured data for the frontend.
        """
        try:
            tools = self._get_tools()
            result = tools.query_health(metric, date_range, computation)
            return {"result": result, "error": None}
        except Exception as exc:
            return {"result": None, "error": str(exc)}

    def list_metrics(self) -> list[str]:
        """Return list of available metric names."""
        return self._query_builder.list_metrics()

    # ------------------------------------------------------------------
    # System
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, Any]:
        """Return system status for the frontend."""
        db_exists = Path(self._db_path).exists()
        return {
            "db_path": self._db_path,
            "db_exists": db_exists,
            "db_size_mb": (
                round(Path(self._db_path).stat().st_size / 1024 / 1024, 1)
                if db_exists
                else 0
            ),
        }
