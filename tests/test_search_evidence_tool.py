"""Tests for MCP search_evidence tool (Tool #10)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import duckdb
from health_platform.mcp.health_tools import HealthTools


def _mock_store(articles=None):
    ms = MagicMock()
    if articles is None:
        articles = [
            {
                "pmid": "39000001",
                "title": "Test",
                "evidence_level": "meta_analysis",
                "evidence_score": 1.0,
            }
        ]
    ms.search.return_value = articles
    ms.format_for_mcp.return_value = "## PubMed Evidence: test\n- **PMID:** 39000001\n"
    return ms


class TestSearchEvidenceTool:
    @patch("health_platform.knowledge.evidence_store.EvidenceStore")
    def test_returns_markdown(self, mock_cls) -> None:
        mock_cls.return_value = _mock_store()
        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS agent")
        tools = HealthTools(con)
        result = tools.search_evidence("magnesium sleep")
        assert "PubMed Evidence" in result
        con.close()

    @patch("health_platform.knowledge.evidence_store.EvidenceStore")
    def test_passes_params(self, mock_cls) -> None:
        ms = _mock_store()
        mock_cls.return_value = ms
        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS agent")
        tools = HealthTools(con)
        tools.search_evidence("HRV sleep", max_results=3, min_year="2020")
        ms.search.assert_called_once_with("HRV sleep", max_results=3, min_year="2020")
        con.close()

    @patch("health_platform.knowledge.evidence_store.EvidenceStore")
    def test_empty_results(self, mock_cls) -> None:
        ms = _mock_store(articles=[])
        ms.format_for_mcp.return_value = "No evidence found for: test"
        mock_cls.return_value = ms
        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS agent")
        tools = HealthTools(con)
        assert "No evidence found" in tools.search_evidence("nonexistent")
        con.close()

    @patch("health_platform.knowledge.evidence_store.EvidenceStore")
    def test_handles_error(self, mock_cls) -> None:
        mock_cls.side_effect = RuntimeError("fail")
        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS agent")
        tools = HealthTools(con)
        result = tools.search_evidence("test")
        assert "Error" in result or "failed" in result.lower()
        con.close()

    @patch("health_platform.knowledge.evidence_store.EvidenceStore")
    def test_default_params(self, mock_cls) -> None:
        ms = _mock_store()
        mock_cls.return_value = ms
        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA IF NOT EXISTS agent")
        tools = HealthTools(con)
        tools.search_evidence("sleep quality")
        ms.search.assert_called_once_with("sleep quality", max_results=5, min_year="")
        con.close()
