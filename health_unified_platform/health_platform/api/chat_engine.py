"""AI-powered chat engine using Claude tool-use and SSE streaming.

Replaces the keyword-routing approach with Claude's native function calling.
The LLM decides which health tools to invoke, executes them in a loop,
and synthesizes a final response from real data.
"""

from __future__ import annotations

import json
import uuid

from health_platform.mcp.health_tools import HealthTools
from health_platform.utils.keychain import get_secret
from health_platform.utils.logging_config import get_logger

logger = get_logger("api.chat_engine")

_client = None  # anthropic.Anthropic, lazily initialized

MAX_TOOL_CALLS = 5

SYSTEM_PROMPT = """You are a personal health assistant with access to the user's real health data from wearable devices (Oura Ring) and other sources.

You have access to health data tools. Use them to fetch the user's actual data before answering. Always query data first, then synthesize insights. Call multiple tools if needed to build a complete picture.

Your role:
- Interpret the user's question and analyze their health data
- Provide clear, actionable insights in a warm but professional tone
- Compare current values against their personal baselines when available
- Flag concerning trends or anomalies
- Use markdown formatting: headers, tables, bullet points, bold for emphasis
- Keep responses concise but informative — think "doctor's summary", not "raw data dump"
- If data shows concerning patterns, mention them but don't diagnose — suggest they consult a doctor
- Respond in the same language as the question (Danish or English)
- When <evidence> tags contain PubMed articles, cite relevant ones using (PMID: XXXXX). Prefer meta-analyses and RCTs over case reports.

Formatting guidelines:
- Use ## for section headers
- Use tables for multi-day data (| day | metric |)
- Use **bold** for key numbers and findings
- Use bullet points for insights and recommendations
- Add a brief summary at the top before detailed data
"""

HEALTH_TOOLS = [
    {
        "name": "query_health",
        "description": "Query a health metric using semantic contracts. Use for questions about sleep, readiness, activity, steps, weight, stress, spo2.",
        "input_schema": {
            "type": "object",
            "properties": {
                "metric": {
                    "type": "string",
                    "description": (
                        "Metric name: sleep_score, readiness_score, steps, "
                        "activity_score, weight, daily_stress, blood_oxygen, "
                        "body_temperature, respiratory_rate, water_intake, "
                        "resting_heart_rate, workout, calories"
                    ),
                },
                "date_range": {
                    "type": "string",
                    "description": (
                        "Date range: today, yesterday, last_7_days, last_30_days, "
                        "last_90_days, or YYYY-MM-DD:YYYY-MM-DD"
                    ),
                },
                "computation": {
                    "type": "string",
                    "enum": ["daily_value", "period_average", "trend", "anomaly"],
                    "description": "Computation type",
                },
            },
            "required": ["metric", "date_range"],
        },
    },
    {
        "name": "get_profile",
        "description": "Load patient profile with demographics, baselines, and personal info.",
        "input_schema": {
            "type": "object",
            "properties": {
                "categories": {
                    "type": "string",
                    "description": "Comma-separated categories: baselines, demographics. Empty for all.",
                }
            },
        },
    },
    {
        "name": "discover_correlations",
        "description": "Find correlations between two health metrics with lag analysis.",
        "input_schema": {
            "type": "object",
            "properties": {
                "metric_a": {
                    "type": "string",
                    "description": "First metric in table.column format",
                },
                "metric_b": {
                    "type": "string",
                    "description": "Second metric in table.column format",
                },
                "max_lag": {
                    "type": "integer",
                    "description": "Max lag days to test (default 3)",
                },
            },
            "required": ["metric_a", "metric_b"],
        },
    },
    {
        "name": "search_memory",
        "description": "Search agent memory and daily summaries for past insights and patterns.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural language search query",
                },
                "top_k": {
                    "type": "integer",
                    "description": "Number of results (default 5)",
                },
            },
            "required": ["query"],
        },
    },
    {
        "name": "search_evidence",
        "description": "Search PubMed for peer-reviewed evidence supporting health claims.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "PubMed search query",
                },
                "max_results": {
                    "type": "integer",
                    "description": "Number of results (default 5, max 20)",
                },
            },
            "required": ["query"],
        },
    },
]


def _get_api_key() -> str:
    """Load Anthropic API key from claude keychain."""
    key = get_secret("ANTHROPIC_API_KEY")
    if key:
        return key
    raise RuntimeError("ANTHROPIC_API_KEY not found in keychain or environment")


def _get_client():
    """Get or create the Anthropic client (lazy import)."""
    import anthropic

    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=_get_api_key())
    return _client


# ------------------------------------------------------------------
# Chat history persistence (DuckDB agent.chat_history)
# ------------------------------------------------------------------


def _ensure_chat_history_table(con) -> None:
    """Create agent.chat_history if it doesn't exist."""
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS agent.chat_history (
            id VARCHAR PRIMARY KEY,
            session_id VARCHAR NOT NULL,
            role VARCHAR NOT NULL,
            content TEXT NOT NULL,
            tool_name VARCHAR,
            tool_input TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    # Create index if not exists — DuckDB supports this syntax
    try:
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_chat_history_session
            ON agent.chat_history(session_id, created_at)
            """
        )
    except Exception:
        # Index may already exist or DuckDB version may not support IF NOT EXISTS
        pass


def _load_chat_history(con, session_id: str, limit: int = 20) -> list[dict]:
    """Load recent messages for a session."""
    try:
        _ensure_chat_history_table(con)
        result = con.execute(
            """
            SELECT role, content, tool_name, tool_input
            FROM agent.chat_history
            WHERE session_id = ?
            ORDER BY created_at ASC
            LIMIT ?
            """,
            [session_id, limit],
        )
        rows = result.fetchall()
    except Exception:
        logger.debug("Failed to load chat history", exc_info=True)
        return []

    messages = []
    for role, content, tool_name, tool_input in rows:
        if role in ("user", "assistant"):
            messages.append({"role": role, "content": content})
        # tool_use and tool_result are reconstructed by the caller if needed
    return messages


def _save_message(
    con,
    session_id: str,
    role: str,
    content: str,
    tool_name: str | None = None,
    tool_input: str | None = None,
) -> None:
    """Save a message to chat history."""
    try:
        _ensure_chat_history_table(con)
        msg_id = str(uuid.uuid4())
        con.execute(
            """
            INSERT INTO agent.chat_history (id, session_id, role, content, tool_name, tool_input)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [msg_id, session_id, role, content, tool_name, tool_input],
        )
    except Exception:
        logger.debug("Failed to save chat message", exc_info=True)


# ------------------------------------------------------------------
# Tool execution dispatcher
# ------------------------------------------------------------------


def _execute_tool(tools: HealthTools, tool_name: str, tool_input: dict) -> str:
    """Execute a health tool and return the result string."""
    try:
        if tool_name == "query_health":
            return tools.query_health(
                tool_input["metric"],
                tool_input["date_range"],
                tool_input.get("computation", "daily_value"),
            )
        elif tool_name == "get_profile":
            categories_str = tool_input.get("categories", "")
            cat_list = (
                [c.strip() for c in categories_str.split(",") if c.strip()]
                if categories_str
                else None
            )
            return tools.get_profile(cat_list)
        elif tool_name == "discover_correlations":
            return tools.discover_correlations(
                tool_input["metric_a"],
                tool_input["metric_b"],
                tool_input.get("max_lag", 3),
            )
        elif tool_name == "search_memory":
            return tools.search_memory(
                tool_input["query"],
                tool_input.get("top_k", 5),
            )
        elif tool_name == "search_evidence":
            return tools.search_evidence(
                tool_input["query"],
                tool_input.get("max_results", 5),
            )
        else:
            return f"Error: Unknown tool '{tool_name}'"
    except Exception as exc:
        logger.error("Tool execution failed: %s(%s) — %s", tool_name, tool_input, exc)
        return f"Error executing {tool_name}: {exc}"


# ------------------------------------------------------------------
# Main response generation (tool-use loop)
# ------------------------------------------------------------------


def generate_response(
    tools: HealthTools, question: str, session_id: str | None = None
) -> str:
    """Generate response using Claude tool-use with multi-turn support.

    1. Load chat history from agent.chat_history if session_id provided
    2. Build messages list with history
    3. Call Claude with tools
    4. If Claude requests tool_use: execute tool, append result, call again
    5. Loop until Claude returns text (max MAX_TOOL_CALLS tool calls)
    6. Save conversation to chat_history
    7. Return final text
    """
    # Build messages with optional history
    messages: list[dict] = []
    if session_id:
        try:
            history = _load_chat_history(tools.con, session_id)
            messages.extend(history)
        except Exception:
            logger.debug("Could not load chat history", exc_info=True)

    # Sanitize question
    safe_question = question.replace("---", "").strip()
    messages.append({"role": "user", "content": safe_question})

    try:
        client = _get_client()
    except ImportError:
        logger.error("anthropic package not installed")
        return (
            "## Setup Required\n\n"
            "The `anthropic` Python package is not installed. "
            "Run: `pip install anthropic`"
        )
    except Exception as exc:
        logger.exception("Failed to initialize Claude client")
        return _format_error_response(exc)

    # Tool-use loop
    tool_call_count = 0
    try:
        while tool_call_count <= MAX_TOOL_CALLS:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                tools=HEALTH_TOOLS,
                messages=messages,
            )

            # Check if response contains tool_use blocks
            tool_use_blocks = [
                block for block in response.content if block.type == "tool_use"
            ]

            if not tool_use_blocks:
                # No tool calls — extract text response
                text_parts = [
                    block.text for block in response.content if block.type == "text"
                ]
                final_text = "\n".join(text_parts) if text_parts else ""

                # Save to history
                if session_id:
                    _save_message(tools.con, session_id, "user", safe_question)
                    _save_message(tools.con, session_id, "assistant", final_text)

                return final_text

            # Process tool calls
            # Add the assistant message with tool_use blocks to messages
            assistant_content = []
            for block in response.content:
                if block.type == "text":
                    assistant_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    assistant_content.append(
                        {
                            "type": "tool_use",
                            "id": block.id,
                            "name": block.name,
                            "input": block.input,
                        }
                    )
            messages.append({"role": "assistant", "content": assistant_content})

            # Execute each tool and build tool_result content
            tool_results = []
            for block in tool_use_blocks:
                result = _execute_tool(tools, block.name, block.input)
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    }
                )
                tool_call_count += 1

                # Save tool interactions to history
                if session_id:
                    _save_message(
                        tools.con,
                        session_id,
                        "tool_use",
                        result,
                        tool_name=block.name,
                        tool_input=json.dumps(block.input),
                    )

            messages.append({"role": "user", "content": tool_results})

        # Hit max tool calls — force a text response
        messages.append(
            {
                "role": "user",
                "content": (
                    "Please provide your final answer now based on the data "
                    "you have gathered so far."
                ),
            }
        )
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=messages,
        )
        text_parts = [block.text for block in response.content if block.type == "text"]
        final_text = "\n".join(text_parts) if text_parts else ""

        if session_id:
            _save_message(tools.con, session_id, "user", safe_question)
            _save_message(tools.con, session_id, "assistant", final_text)

        return final_text

    except Exception as exc:
        return _format_error_response(exc)


# ------------------------------------------------------------------
# SSE Streaming response generation
# ------------------------------------------------------------------


def generate_response_stream(
    tools: HealthTools, question: str, session_id: str | None = None
):
    """Generator that yields text chunks for SSE streaming.

    Same tool-use loop as generate_response but uses stream=True for the
    final text response. Tool calls are executed silently; only the final
    synthesized answer is streamed to the client.
    """
    # Build messages with optional history
    messages: list[dict] = []
    if session_id:
        try:
            history = _load_chat_history(tools.con, session_id)
            messages.extend(history)
        except Exception:
            logger.debug("Could not load chat history", exc_info=True)

    safe_question = question.replace("---", "").strip()
    messages.append({"role": "user", "content": safe_question})

    try:
        client = _get_client()
    except ImportError:
        yield (
            "## Setup Required\n\n"
            "The `anthropic` Python package is not installed. "
            "Run: `pip install anthropic`"
        )
        return
    except Exception as exc:
        yield _format_error_response(exc)
        return

    # Tool-use loop (non-streaming) — resolve all tool calls first
    tool_call_count = 0
    try:
        while tool_call_count <= MAX_TOOL_CALLS:
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                tools=HEALTH_TOOLS,
                messages=messages,
            )

            tool_use_blocks = [
                block for block in response.content if block.type == "tool_use"
            ]

            if not tool_use_blocks:
                # No more tool calls — now stream the final response
                break

            # Process tool calls (same as generate_response)
            assistant_content = []
            for block in response.content:
                if block.type == "text":
                    assistant_content.append({"type": "text", "text": block.text})
                elif block.type == "tool_use":
                    assistant_content.append(
                        {
                            "type": "tool_use",
                            "id": block.id,
                            "name": block.name,
                            "input": block.input,
                        }
                    )
            messages.append({"role": "assistant", "content": assistant_content})

            tool_results = []
            for block in tool_use_blocks:
                result = _execute_tool(tools, block.name, block.input)
                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    }
                )
                tool_call_count += 1

                if session_id:
                    _save_message(
                        tools.con,
                        session_id,
                        "tool_use",
                        result,
                        tool_name=block.name,
                        tool_input=json.dumps(block.input),
                    )

            messages.append({"role": "user", "content": tool_results})
        else:
            # Hit max tool calls — add prompt for final answer
            messages.append(
                {
                    "role": "user",
                    "content": (
                        "Please provide your final answer now based on the data "
                        "you have gathered so far."
                    ),
                }
            )

        # Stream the final response
        full_text = ""
        with client.messages.stream(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=messages,
        ) as stream:
            for text in stream.text_stream:
                full_text += text
                yield text

        # Save to history after streaming completes
        if session_id:
            _save_message(tools.con, session_id, "user", safe_question)
            _save_message(tools.con, session_id, "assistant", full_text)

    except Exception as exc:
        yield _format_error_response(exc)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _format_error_response(exc: Exception) -> str:
    """Format an error into a user-friendly markdown response."""
    exc_type = type(exc).__name__
    if exc_type in ("AuthenticationError", "PermissionDeniedError"):
        logger.error("Invalid ANTHROPIC_API_KEY")
        return (
            "## Configuration Error\n\n"
            "The AI assistant is not configured correctly. "
            "Please check the ANTHROPIC_API_KEY in your keychain."
        )
    logger.exception("Claude API call failed")
    return (
        "## Temporary Error\n\n"
        "Could not reach the AI assistant. "
        "Your health data is still accessible via the quick action buttons."
    )
