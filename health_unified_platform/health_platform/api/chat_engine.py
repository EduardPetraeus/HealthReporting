"""AI-powered chat engine using Claude API.

Takes a natural language health question, gathers relevant data from
the health database, and uses Claude to generate an intelligent,
contextual response with insights — like a personal health advisor.
"""

from __future__ import annotations

import os
import subprocess

from health_platform.mcp.health_tools import HealthTools
from health_platform.utils.logging_config import get_logger

logger = get_logger("api.chat_engine")

_client = None  # anthropic.Anthropic, lazily initialized

SYSTEM_PROMPT = """You are a personal health assistant with access to the user's real health data from wearable devices (Oura Ring) and other sources.

Your role:
- Interpret the user's question and analyze their health data
- Provide clear, actionable insights in a warm but professional tone
- Compare current values against their personal baselines when available
- Flag concerning trends or anomalies
- Use markdown formatting: headers, tables, bullet points, bold for emphasis
- Keep responses concise but informative — think "doctor's summary", not "raw data dump"
- If data shows concerning patterns, mention them but don't diagnose — suggest they consult a doctor
- Respond in the same language as the question (Danish or English)

Formatting guidelines:
- Use ## for section headers
- Use tables for multi-day data (| day | metric |)
- Use **bold** for key numbers and findings
- Use bullet points for insights and recommendations
- Add a brief summary at the top before detailed data
"""


def _get_api_key() -> str:
    """Load Anthropic API key from claude keychain."""
    try:
        result = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-a",
                "claude",
                "-s",
                "ANTHROPIC_API_KEY",
                "-w",
                os.path.expanduser("~/Library/Keychains/claude.keychain-db"),
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception as exc:
        logger.debug("Keychain lookup for ANTHROPIC_API_KEY failed: %s", exc)

    key = os.environ.get("ANTHROPIC_API_KEY", "")
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


def _gather_context(tools: HealthTools, question: str) -> str:
    """Gather relevant health data context for the AI based on the question."""
    q = question.lower()
    context_parts = []

    # Always include patient profile (core memory)
    try:
        profile = tools.get_profile()
        if profile and "No profile" not in profile:
            context_parts.append("## Patient Profile\n" + profile)
    except Exception:
        pass

    # Gather data based on question keywords
    data_queries = []

    # Sleep
    if any(kw in q for kw in ["sleep", "sov", "søvn", "slept", "insomnia"]):
        data_queries.extend(
            [
                ("sleep_score", "last_7_days", "daily_value"),
                ("sleep_score", "last_30_days", "period_average"),
                ("sleep_score", "last_30_days", "trend"),
            ]
        )

    # Readiness
    elif any(kw in q for kw in ["readiness", "ready", "klar", "energy", "energi"]):
        data_queries.extend(
            [
                ("readiness_score", "last_7_days", "daily_value"),
                ("readiness_score", "last_30_days", "period_average"),
            ]
        )

    # Steps / Activity
    elif any(kw in q for kw in ["step", "skridt", "walk", "activity", "aktiv"]):
        data_queries.extend(
            [
                ("steps", "last_7_days", "daily_value"),
                ("steps", "last_30_days", "period_average"),
                ("activity_score", "last_7_days", "daily_value"),
            ]
        )

    # Workout
    elif any(kw in q for kw in ["workout", "exercise", "træning", "run", "løb"]):
        data_queries.append(("workout", "last_30_days", "daily_value"))

    # Stress
    elif any(kw in q for kw in ["stress", "recover", "afslapning"]):
        data_queries.append(("daily_stress", "last_7_days", "daily_value"))

    # Weight
    elif any(kw in q for kw in ["weight", "vægt", "body", "krop"]):
        data_queries.append(("weight", "last_30_days", "daily_value"))

    # Broad / overview / trend questions — give everything
    else:
        data_queries.extend(
            [
                ("sleep_score", "last_7_days", "daily_value"),
                ("readiness_score", "last_7_days", "daily_value"),
                ("steps", "last_7_days", "daily_value"),
                ("sleep_score", "last_30_days", "period_average"),
                ("readiness_score", "last_30_days", "period_average"),
            ]
        )

    # Execute queries
    for metric, date_range, computation in data_queries:
        try:
            result = tools.query_health(metric, date_range, computation)
            if result and "Error" not in result:
                label = f"{metric} ({date_range}, {computation})"
                context_parts.append(f"## {label}\n{result}")
        except Exception:
            pass

    # Try to get recent daily summaries for richer context
    try:
        summaries = tools.search_memory(question)
        if summaries and "No results" not in summaries:
            context_parts.append("## Recent Daily Summaries\n" + summaries)
    except Exception:
        pass

    return (
        "\n\n---\n\n".join(context_parts)
        if context_parts
        else "No health data available."
    )


def generate_response(tools: HealthTools, question: str) -> str:
    """Generate an AI-powered response to a health question.

    1. Gathers relevant health data from the database
    2. Sends context + question to Claude
    3. Returns a rich markdown response with insights
    """
    # Gather data context
    context = _gather_context(tools, question)

    # Build the user message with context
    user_message = (
        f"Here is the user's current health data:\n\n"
        f"{context}\n\n"
        f"---\n\n"
        f"User's question: {question}"
    )

    try:
        client = _get_client()
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_message}],
        )
        return response.content[0].text
    except ImportError:
        logger.error("anthropic package not installed")
        return (
            "## Setup Required\n\n"
            "The `anthropic` Python package is not installed. "
            "Run: `pip install anthropic`"
        )
    except Exception as exc:
        if "authentication" in str(exc).lower() or "api_key" in str(exc).lower():
            logger.error("Invalid ANTHROPIC_API_KEY")
            return (
                "## Configuration Error\n\n"
                "The AI assistant is not configured correctly. "
                "Please check the ANTHROPIC_API_KEY in your keychain."
            )
        logger.error("Claude API call failed: %s", exc)
        return (
            "## Temporary Error\n\n"
            "Could not reach the AI assistant. "
            "Your health data is still accessible via the quick action buttons.\n\n"
            f"*Technical: {type(exc).__name__}*"
        )
