"""AI-powered chat engine using Claude API.

Takes a natural language health question, gathers relevant data from
the health database, and uses Claude to generate an intelligent,
contextual response with insights — like a personal health advisor.
"""

from __future__ import annotations

from health_platform.mcp.health_tools import HealthTools
from health_platform.utils.keychain import get_secret
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
- When <evidence> tags contain PubMed articles, cite relevant ones using (PMID: XXXXX). Prefer meta-analyses and RCTs over case reports.

Formatting guidelines:
- Use ## for section headers
- Use tables for multi-day data (| day | metric |)
- Use **bold** for key numbers and findings
- Use bullet points for insights and recommendations
- Add a brief summary at the top before detailed data
"""


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


_EVIDENCE_KEYWORDS: dict[str, str] = {
    "sleep": "sleep quality health outcomes",
    "søvn": "sleep quality health outcomes",
    "sov": "sleep quality health outcomes",
    "hrv": "heart rate variability health",
    "heart rate": "heart rate variability health",
    "puls": "heart rate variability health",
    "stress": "stress biomarkers cortisol health",
    "readiness": "recovery readiness biomarkers",
    "recovery": "recovery readiness biomarkers",
    "steps": "physical activity step count health benefits",
    "skridt": "physical activity step count health benefits",
    "exercise": "exercise health outcomes meta-analysis",
    "træning": "exercise health outcomes meta-analysis",
    "weight": "body weight management health",
    "vægt": "body weight management health",
    "magnesium": "magnesium supplementation health",
    "vitamin": "vitamin supplementation health outcomes",
    "caffeine": "caffeine health effects",
    "kaffe": "caffeine health effects",
    "alcohol": "alcohol health effects",
    "alkohol": "alcohol health effects",
    "melatonin": "melatonin sleep supplementation",
}


def _build_evidence_query(question_lower: str) -> str | None:
    """Map question keywords to PubMed search queries. Returns None if no match."""
    matched_queries: list[str] = []
    seen: set[str] = set()
    for keyword, query in _EVIDENCE_KEYWORDS.items():
        if keyword in question_lower and query not in seen:
            matched_queries.append(query)
            seen.add(query)
        if len(matched_queries) >= 2:
            break
    if not matched_queries:
        return None
    return " OR ".join(matched_queries)


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
        logger.debug("Failed to load patient profile", exc_info=True)

    # Gather data based on question keywords
    data_queries = []

    matched = False

    # Sleep
    if any(kw in q for kw in ["sleep", "sov", "søvn", "slept", "insomnia"]):
        matched = True
        data_queries.extend(
            [
                ("sleep_score", "last_7_days", "daily_value"),
                ("sleep_score", "last_30_days", "period_average"),
                ("sleep_score", "last_30_days", "trend"),
            ]
        )

    # Readiness
    if any(kw in q for kw in ["readiness", "ready", "klar", "energy", "energi"]):
        matched = True
        data_queries.extend(
            [
                ("readiness_score", "last_7_days", "daily_value"),
                ("readiness_score", "last_30_days", "period_average"),
            ]
        )

    # Steps / Activity
    if any(kw in q for kw in ["step", "skridt", "walk", "activity", "aktiv"]):
        matched = True
        data_queries.extend(
            [
                ("steps", "last_7_days", "daily_value"),
                ("steps", "last_30_days", "period_average"),
                ("activity_score", "last_7_days", "daily_value"),
            ]
        )

    # Workout
    if any(kw in q for kw in ["workout", "exercise", "træning", "run", "løb"]):
        matched = True
        data_queries.append(("workout", "last_30_days", "daily_value"))

    # Stress
    if any(kw in q for kw in ["stress", "recover", "afslapning"]):
        matched = True
        data_queries.append(("daily_stress", "last_7_days", "daily_value"))

    # Weight
    if any(kw in q for kw in ["weight", "vægt", "body", "krop"]):
        matched = True
        data_queries.append(("weight", "last_30_days", "daily_value"))

    # Broad / overview / trend questions — give everything
    if not matched:
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
            logger.debug("Failed to query %s", metric, exc_info=True)

    # Gather PubMed evidence if relevant keywords detected
    evidence_query = _build_evidence_query(q)
    if evidence_query:
        try:
            from health_platform.knowledge.evidence_store import EvidenceStore

            store = EvidenceStore(tools.con)
            articles = store.search(evidence_query, max_results=3)
            evidence_block = store.format_for_context(articles)
            if evidence_block:
                context_parts.append(evidence_block)
        except Exception:
            logger.debug("Failed to gather evidence", exc_info=True)

    # Try to get recent daily summaries for richer context
    try:
        summaries = tools.search_memory(question)
        if summaries and "No results" not in summaries:
            context_parts.append("## Recent Daily Summaries\n" + summaries)
    except Exception:
        logger.debug("Failed to search memory", exc_info=True)

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

    # Sanitize question: strip patterns that could manipulate the prompt
    safe_question = question.replace("---", "").strip()

    # Build the user message with context — clear separation between data and question
    user_message = (
        f"<health_data>\n{context}\n</health_data>\n\n"
        f"<user_question>\n{safe_question}\n</user_question>"
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
        exc_type = type(exc).__name__
        # Detect auth errors by exception type name (avoids importing anthropic)
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
