"""Tests for evidence integration in the chat engine."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "health_unified_platform"))

from health_platform.api.chat_engine import _build_evidence_query


class TestBuildEvidenceQuery:
    def test_sleep_keyword_maps_to_query(self) -> None:
        result = _build_evidence_query("how does magnesium affect my sleep?")
        assert result is not None

    def test_no_health_keywords_returns_none(self) -> None:
        assert _build_evidence_query("what time is it?") is None

    def test_multiple_keywords_combined(self) -> None:
        result = _build_evidence_query("how does caffeine and alcohol affect sleep?")
        assert result is not None
        assert "OR" in result

    def test_danish_keywords_work(self) -> None:
        result = _build_evidence_query("hvordan p\u00e5virker kaffe min s\u00f8vn?")
        assert result is not None

    def test_hrv_keyword(self) -> None:
        result = _build_evidence_query("what does my hrv trend mean?")
        assert result is not None
        assert "heart rate variability" in result.lower()

    def test_exercise_keyword(self) -> None:
        result = _build_evidence_query("how does exercise affect recovery?")
        assert result is not None

    def test_max_two_queries_combined(self) -> None:
        result = _build_evidence_query(
            "how do sleep exercise and hrv relate to stress?"
        )
        assert result is not None
        assert result.count(" OR ") <= 1
