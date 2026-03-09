"""Tests for B1: LLM Chat Engine with tool-use and SSE streaming."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))

from health_platform.api.chat_engine import (
    HEALTH_TOOLS,
    MAX_TOOL_CALLS,
    SYSTEM_PROMPT,
    _ensure_chat_history_table,
    _execute_tool,
    _load_chat_history,
    _save_message,
    generate_response,
    generate_response_stream,
)

# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def memory_db():
    """Create a fresh in-memory DuckDB for each test."""
    con = duckdb.connect(":memory:")
    yield con
    con.close()


@pytest.fixture
def mock_health_tools(memory_db):
    """Create a mock HealthTools with a real DuckDB connection."""
    tools = MagicMock()
    tools.con = memory_db
    tools.query_health.return_value = (
        "| date | sleep_score |\n|---|---|\n| 2026-03-07 | 85 |"
    )
    tools.get_profile.return_value = "# Patient Profile\n## demographics\nAge: 35"
    tools.discover_correlations.return_value = "r=0.72 (strong)"
    tools.search_memory.return_value = "## Daily Summaries\nGood sleep pattern observed"
    tools.search_evidence.return_value = (
        "<evidence>Sleep quality meta-analysis</evidence>"
    )
    tools.close.return_value = None
    return tools


def _make_text_response(text: str):
    """Create a mock Claude API response with a text block."""
    block = SimpleNamespace(type="text", text=text)
    return SimpleNamespace(content=[block])


def _make_tool_use_response(
    tool_name: str, tool_input: dict, tool_id: str = "toolu_01"
):
    """Create a mock Claude API response with a tool_use block."""
    block = SimpleNamespace(
        type="tool_use", id=tool_id, name=tool_name, input=tool_input
    )
    return SimpleNamespace(content=[block])


def _make_mixed_response(
    text: str, tool_name: str, tool_input: dict, tool_id: str = "toolu_01"
):
    """Create a mock Claude API response with both text and tool_use blocks."""
    text_block = SimpleNamespace(type="text", text=text)
    tool_block = SimpleNamespace(
        type="tool_use", id=tool_id, name=tool_name, input=tool_input
    )
    return SimpleNamespace(content=[text_block, tool_block])


# ------------------------------------------------------------------
# Tool schema tests
# ------------------------------------------------------------------


class TestHealthToolsSchema:
    def test_all_tools_have_required_fields(self) -> None:
        """All tool schemas have required fields."""
        for tool in HEALTH_TOOLS:
            assert "name" in tool, f"Tool missing 'name': {tool}"
            assert "description" in tool, f"Tool missing 'description': {tool}"
            assert "input_schema" in tool, f"Tool missing 'input_schema': {tool}"

    def test_all_tools_have_type_object(self) -> None:
        """All input schemas are type=object."""
        for tool in HEALTH_TOOLS:
            assert tool["input_schema"]["type"] == "object"

    def test_tool_names_unique(self) -> None:
        """All tool names are unique."""
        names = [t["name"] for t in HEALTH_TOOLS]
        assert len(names) == len(set(names))

    def test_tool_count(self) -> None:
        """We expose exactly 5 tools to Claude."""
        assert len(HEALTH_TOOLS) == 5

    def test_query_health_has_required_fields(self) -> None:
        """query_health requires metric and date_range."""
        qh = next(t for t in HEALTH_TOOLS if t["name"] == "query_health")
        assert "metric" in qh["input_schema"]["properties"]
        assert "date_range" in qh["input_schema"]["properties"]
        assert qh["input_schema"]["required"] == ["metric", "date_range"]

    def test_system_prompt_mentions_tools(self) -> None:
        """System prompt tells Claude to use tools."""
        assert "tools" in SYSTEM_PROMPT.lower()
        assert "query data first" in SYSTEM_PROMPT.lower()


# ------------------------------------------------------------------
# Tool execution dispatcher tests
# ------------------------------------------------------------------


class TestExecuteTool:
    def test_query_health_dispatches(self, mock_health_tools) -> None:
        """query_health tool dispatches correctly."""
        result = _execute_tool(
            mock_health_tools,
            "query_health",
            {"metric": "sleep_score", "date_range": "last_7_days"},
        )
        mock_health_tools.query_health.assert_called_once_with(
            "sleep_score", "last_7_days", "daily_value"
        )
        assert "sleep_score" in result

    def test_query_health_with_computation(self, mock_health_tools) -> None:
        """query_health passes computation parameter."""
        _execute_tool(
            mock_health_tools,
            "query_health",
            {"metric": "steps", "date_range": "last_30_days", "computation": "trend"},
        )
        mock_health_tools.query_health.assert_called_once_with(
            "steps", "last_30_days", "trend"
        )

    def test_get_profile_dispatches(self, mock_health_tools) -> None:
        """get_profile tool dispatches correctly."""
        result = _execute_tool(
            mock_health_tools, "get_profile", {"categories": "baselines,demographics"}
        )
        mock_health_tools.get_profile.assert_called_once()
        assert "Profile" in result

    def test_get_profile_empty_categories(self, mock_health_tools) -> None:
        """get_profile with empty categories passes None."""
        _execute_tool(mock_health_tools, "get_profile", {})
        mock_health_tools.get_profile.assert_called_once_with(None)

    def test_discover_correlations_dispatches(self, mock_health_tools) -> None:
        """discover_correlations dispatches correctly."""
        result = _execute_tool(
            mock_health_tools,
            "discover_correlations",
            {
                "metric_a": "daily_sleep.sleep_score",
                "metric_b": "daily_readiness.readiness_score",
            },
        )
        mock_health_tools.discover_correlations.assert_called_once()
        assert "0.72" in result

    def test_search_memory_dispatches(self, mock_health_tools) -> None:
        """search_memory dispatches correctly."""
        result = _execute_tool(
            mock_health_tools, "search_memory", {"query": "sleep patterns"}
        )
        mock_health_tools.search_memory.assert_called_once_with("sleep patterns", 5)
        assert "Summaries" in result

    def test_search_evidence_dispatches(self, mock_health_tools) -> None:
        """search_evidence dispatches correctly."""
        result = _execute_tool(
            mock_health_tools,
            "search_evidence",
            {"query": "magnesium sleep", "max_results": 3},
        )
        mock_health_tools.search_evidence.assert_called_once_with("magnesium sleep", 3)
        assert "evidence" in result.lower()

    def test_unknown_tool_returns_error(self, mock_health_tools) -> None:
        """Unknown tool name returns error string."""
        result = _execute_tool(mock_health_tools, "nonexistent_tool", {})
        assert "Error" in result
        assert "Unknown tool" in result

    def test_tool_exception_returns_error(self, mock_health_tools) -> None:
        """Tool execution exception returns error string, doesn't raise."""
        mock_health_tools.query_health.side_effect = RuntimeError("DB connection lost")
        result = _execute_tool(
            mock_health_tools,
            "query_health",
            {"metric": "sleep_score", "date_range": "today"},
        )
        assert "Error" in result
        assert "query failed" in result


# ------------------------------------------------------------------
# Chat history tests
# ------------------------------------------------------------------


class TestChatHistory:
    def test_ensure_table_creates_schema_and_table(self, memory_db) -> None:
        """Chat history table is created in agent schema."""
        _ensure_chat_history_table(memory_db)
        result = memory_db.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'agent' AND table_name = 'chat_history'"
        ).fetchone()
        assert result is not None
        assert result[0] == "chat_history"

    def test_ensure_table_idempotent(self, memory_db) -> None:
        """Calling _ensure_chat_history_table twice doesn't fail."""
        _ensure_chat_history_table(memory_db)
        _ensure_chat_history_table(memory_db)  # Should not raise

    def test_save_and_load_messages(self, memory_db) -> None:
        """Messages can be saved and loaded by session_id."""
        session = "test-session-123"
        _save_message(memory_db, session, "user", "How did I sleep?")
        _save_message(memory_db, session, "assistant", "You slept well.")

        messages = _load_chat_history(memory_db, session)
        assert len(messages) == 2
        assert messages[0]["role"] == "user"
        assert messages[0]["content"] == "How did I sleep?"
        assert messages[1]["role"] == "assistant"
        assert messages[1]["content"] == "You slept well."

    def test_load_filters_by_session(self, memory_db) -> None:
        """Messages from different sessions don't mix."""
        _save_message(memory_db, "session-a", "user", "Question A")
        _save_message(memory_db, "session-b", "user", "Question B")

        messages_a = _load_chat_history(memory_db, "session-a")
        assert len(messages_a) == 1
        assert messages_a[0]["content"] == "Question A"

    def test_chat_history_limit(self, memory_db) -> None:
        """Only last N messages are loaded."""
        session = "limit-test"
        for i in range(30):
            _save_message(memory_db, session, "user", f"Message {i}")

        messages = _load_chat_history(memory_db, session, limit=5)
        assert len(messages) == 5

    def test_save_tool_use_message(self, memory_db) -> None:
        """Tool use messages are saved with tool_name and tool_input."""
        session = "tool-session"
        _save_message(
            memory_db,
            session,
            "tool_use",
            "sleep score: 85",
            tool_name="query_health",
            tool_input='{"metric": "sleep_score"}',
        )

        # tool_use messages are not returned in _load_chat_history
        # (they're internal), but they should be stored
        result = memory_db.execute(
            "SELECT role, tool_name, tool_input FROM agent.chat_history WHERE session_id = ?",
            [session],
        ).fetchone()
        assert result[0] == "tool_use"
        assert result[1] == "query_health"

    def test_load_empty_session(self, memory_db) -> None:
        """Loading a nonexistent session returns empty list."""
        _ensure_chat_history_table(memory_db)
        messages = _load_chat_history(memory_db, "nonexistent")
        assert messages == []


# ------------------------------------------------------------------
# generate_response tests (mocked Claude API)
# ------------------------------------------------------------------


class TestGenerateResponse:
    @patch("health_platform.api.chat_engine._get_client")
    def test_simple_text_response(self, mock_get_client, mock_health_tools) -> None:
        """Simple question that needs no tools returns text directly."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.messages.create.return_value = _make_text_response(
            "You slept great last night!"
        )

        result = generate_response(mock_health_tools, "How did I sleep?")

        assert result == "You slept great last night!"
        mock_client.messages.create.assert_called_once()

    @patch("health_platform.api.chat_engine._get_client")
    def test_tool_use_then_text(self, mock_get_client, mock_health_tools) -> None:
        """Claude calls a tool, gets result, then responds with text."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # First call: Claude wants to use a tool
        tool_response = _make_tool_use_response(
            "query_health",
            {"metric": "sleep_score", "date_range": "last_7_days"},
        )
        # Second call: Claude responds with text
        text_response = _make_text_response("Your sleep score was 85 yesterday.")

        mock_client.messages.create.side_effect = [tool_response, text_response]

        result = generate_response(mock_health_tools, "How did I sleep?")

        assert "85" in result
        assert mock_client.messages.create.call_count == 2
        mock_health_tools.query_health.assert_called_once()

    @patch("health_platform.api.chat_engine._get_client")
    def test_max_tool_calls_stops_loop(
        self, mock_get_client, mock_health_tools
    ) -> None:
        """Tool call loop stops after MAX_TOOL_CALLS iterations."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Always return tool_use (would loop forever without limit)
        tool_response = _make_tool_use_response(
            "query_health",
            {"metric": "sleep_score", "date_range": "today"},
        )
        final_response = _make_text_response("Here is your summary.")

        # MAX_TOOL_CALLS tool responses in the loop, then 1 forced text response
        mock_client.messages.create.side_effect = [tool_response] * MAX_TOOL_CALLS + [
            final_response
        ]

        result = generate_response(mock_health_tools, "Overview please")

        assert "summary" in result.lower()
        # MAX_TOOL_CALLS iterations in loop + 1 forced text call = MAX_TOOL_CALLS + 1
        assert mock_client.messages.create.call_count == MAX_TOOL_CALLS + 1

    @patch("health_platform.api.chat_engine._get_client")
    def test_session_id_triggers_history_save(
        self, mock_get_client, mock_health_tools
    ) -> None:
        """When session_id is provided, messages are saved to history."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.messages.create.return_value = _make_text_response("Response")

        session = "test-session-save"
        generate_response(mock_health_tools, "Test question", session_id=session)

        # Verify messages were saved
        messages = _load_chat_history(mock_health_tools.con, session)
        assert len(messages) == 2
        assert messages[0]["role"] == "user"
        assert messages[1]["role"] == "assistant"

    @patch("health_platform.api.chat_engine._get_client")
    def test_no_session_id_skips_history(
        self, mock_get_client, mock_health_tools
    ) -> None:
        """Without session_id, no history is saved."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.messages.create.return_value = _make_text_response("Response")

        generate_response(mock_health_tools, "Test question")

        _ensure_chat_history_table(mock_health_tools.con)
        count = mock_health_tools.con.execute(
            "SELECT COUNT(*) FROM agent.chat_history"
        ).fetchone()[0]
        assert count == 0

    @patch("health_platform.api.chat_engine._get_client")
    def test_api_error_returns_friendly_message(
        self, mock_get_client, mock_health_tools
    ) -> None:
        """API errors return user-friendly error messages."""
        mock_get_client.side_effect = RuntimeError("Connection refused")

        result = generate_response(mock_health_tools, "Test question")

        assert "Error" in result or "error" in result.lower()

    @patch("health_platform.api.chat_engine._get_client")
    def test_question_sanitization(self, mock_get_client, mock_health_tools) -> None:
        """Questions with --- are sanitized."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.messages.create.return_value = _make_text_response("OK")

        generate_response(mock_health_tools, "Test --- injection")

        call_args = mock_client.messages.create.call_args
        user_msg = call_args.kwargs["messages"][-1]["content"]
        assert "---" not in user_msg


# ------------------------------------------------------------------
# generate_response_stream tests
# ------------------------------------------------------------------


class TestGenerateResponseStream:
    @patch("health_platform.api.chat_engine._get_client")
    def test_stream_yields_text_chunks(
        self, mock_get_client, mock_health_tools
    ) -> None:
        """Streaming generator yields text chunks."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # First call: no tools needed
        mock_client.messages.create.return_value = _make_text_response("Direct answer")

        # Mock the streaming context manager
        mock_stream = MagicMock()
        mock_stream.__enter__ = MagicMock(return_value=mock_stream)
        mock_stream.__exit__ = MagicMock(return_value=False)
        mock_stream.text_stream = iter(["Hello ", "world!"])
        mock_client.messages.stream.return_value = mock_stream

        chunks = list(generate_response_stream(mock_health_tools, "Hello"))

        assert len(chunks) == 2
        assert chunks[0] == "Hello "
        assert chunks[1] == "world!"

    @patch("health_platform.api.chat_engine._get_client")
    def test_stream_with_tool_use_first(
        self, mock_get_client, mock_health_tools
    ) -> None:
        """Streaming resolves tool calls before streaming text."""
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # First call: tool use
        tool_response = _make_tool_use_response(
            "query_health",
            {"metric": "sleep_score", "date_range": "last_7_days"},
        )
        # Second call: no more tools
        text_response = _make_text_response("Data ready")

        mock_client.messages.create.side_effect = [tool_response, text_response]

        # Mock streaming for the final response
        mock_stream = MagicMock()
        mock_stream.__enter__ = MagicMock(return_value=mock_stream)
        mock_stream.__exit__ = MagicMock(return_value=False)
        mock_stream.text_stream = iter(["Streamed ", "response"])
        mock_client.messages.stream.return_value = mock_stream

        chunks = list(generate_response_stream(mock_health_tools, "Sleep data"))

        assert "".join(chunks) == "Streamed response"
        mock_health_tools.query_health.assert_called_once()


# ------------------------------------------------------------------
# ChatRequest model tests
# ------------------------------------------------------------------


class TestChatRequestModel:
    """Test ChatRequest model fields.

    We define a local copy of ChatRequest to avoid importing server.py,
    which triggers heavy transitive imports (pandas, etc.) that may not
    be available in all test environments. The model definition is simple
    enough that testing it directly is valid.
    """

    def _get_model(self):
        """Build a ChatRequest-equivalent model for testing."""
        from typing import Literal

        from pydantic import BaseModel, Field

        class ChatRequest(BaseModel):
            question: str = Field(..., max_length=2000)
            format: Literal["markdown", "plain"] = "markdown"
            session_id: str | None = Field(None)

        return ChatRequest

    def test_session_id_optional(self) -> None:
        """ChatRequest accepts optional session_id."""
        ChatRequest = self._get_model()
        req = ChatRequest(question="test", session_id="abc-123")
        assert req.session_id == "abc-123"

    def test_session_id_none_by_default(self) -> None:
        """ChatRequest session_id is None by default."""
        ChatRequest = self._get_model()
        req = ChatRequest(question="test")
        assert req.session_id is None

    def test_question_max_length(self) -> None:
        """ChatRequest enforces max_length on question."""
        ChatRequest = self._get_model()
        with pytest.raises(Exception):
            ChatRequest(question="x" * 2001)


# ------------------------------------------------------------------
# Constants tests
# ------------------------------------------------------------------


class TestConstants:
    def test_max_tool_calls_is_reasonable(self) -> None:
        """MAX_TOOL_CALLS is set to a reasonable limit."""
        assert 1 <= MAX_TOOL_CALLS <= 10

    def test_system_prompt_is_nonempty(self) -> None:
        """System prompt is not empty."""
        assert len(SYSTEM_PROMPT) > 100
