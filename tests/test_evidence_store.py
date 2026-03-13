"""Tests for evidence cache and retrieval layer."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock

import duckdb
import pytest
from health_platform.knowledge.evidence_store import EvidenceStore
from health_platform.knowledge.pubmed_client import PubMedClient

pytestmark = pytest.mark.integration


def _make_article(
    pmid="12345678", title="Test Article", evidence_level="rct", evidence_score=0.85
):
    return {
        "pmid": pmid,
        "title": title,
        "abstract": "Test abstract for unit testing.",
        "authors": "Smith J, Jones A",
        "journal": "Test Journal",
        "pub_date": "2024 Jan",
        "pub_year": 2024,
        "doi": f"10.1234/test.{pmid}",
        "publication_types": ["Randomized Controlled Trial"],
        "mesh_terms": ["Sleep", "Heart Rate"],
        "evidence_level": evidence_level,
        "evidence_score": evidence_score,
    }


class TestEvidenceStoreCache:
    def test_cache_miss_calls_pubmed(self) -> None:
        con = duckdb.connect(":memory:")
        mc = MagicMock(spec=PubMedClient)
        mc.search.return_value = ["12345678"]
        mc.fetch_articles.return_value = [_make_article()]
        store = EvidenceStore(con, client=mc)
        results = store.search("test query", max_results=1)
        mc.search.assert_called_once()
        assert len(results) >= 1
        con.close()

    def test_cache_hit_skips_pubmed(self) -> None:
        con = duckdb.connect(":memory:")
        mc = MagicMock(spec=PubMedClient)
        mc.search.return_value = ["12345678"]
        mc.fetch_articles.return_value = [_make_article()]
        store = EvidenceStore(con, client=mc)
        store.search("test query", max_results=1)
        mc.search.reset_mock()
        mc.fetch_articles.reset_mock()
        results = store.search("test query", max_results=1)
        mc.search.assert_not_called()
        assert len(results) == 1
        con.close()

    def test_cache_normalizes_query(self) -> None:
        con = duckdb.connect(":memory:")
        mc = MagicMock(spec=PubMedClient)
        mc.search.return_value = ["12345678"]
        mc.fetch_articles.return_value = [_make_article()]
        store = EvidenceStore(con, client=mc)
        store.search("  Test   QUERY  ", max_results=1)
        mc.search.reset_mock()
        results = store.search("test query", max_results=1)
        mc.search.assert_not_called()
        assert len(results) == 1
        con.close()

    def test_results_sorted_by_evidence_score(self) -> None:
        con = duckdb.connect(":memory:")
        mc = MagicMock(spec=PubMedClient)
        mc.search.return_value = ["111", "222", "333"]
        mc.fetch_articles.return_value = [
            _make_article(pmid="111", evidence_level="case_report", evidence_score=0.4),
            _make_article(
                pmid="222", evidence_level="meta_analysis", evidence_score=1.0
            ),
            _make_article(pmid="333", evidence_level="rct", evidence_score=0.85),
        ]
        store = EvidenceStore(con, client=mc)
        results = store.search("sleep hrv", max_results=3)
        scores = [r["evidence_score"] for r in results]
        assert scores == sorted(scores, reverse=True)
        con.close()


class TestEvidenceStoreArticleLookup:
    def test_get_article_returns_cached(self) -> None:
        con = duckdb.connect(":memory:")
        mc = MagicMock(spec=PubMedClient)
        mc.search.return_value = ["12345678"]
        mc.fetch_articles.return_value = [_make_article()]
        store = EvidenceStore(con, client=mc)
        store.search("test query", max_results=1)
        article = store.get_article("12345678")
        assert article is not None
        assert article["pmid"] == "12345678"
        con.close()

    def test_get_article_returns_none_for_missing(self) -> None:
        con = duckdb.connect(":memory:")
        store = EvidenceStore(con, client=MagicMock(spec=PubMedClient))
        assert store.get_article("99999999") is None
        con.close()


class TestEvidenceStoreFormatting:
    def test_format_for_mcp_markdown(self) -> None:
        con = duckdb.connect(":memory:")
        store = EvidenceStore(con, client=MagicMock(spec=PubMedClient))
        output = store.format_for_mcp([_make_article()], "test query")
        assert "## PubMed Evidence" in output
        assert "PMID:" in output
        con.close()

    def test_format_for_mcp_empty(self) -> None:
        con = duckdb.connect(":memory:")
        store = EvidenceStore(con, client=MagicMock(spec=PubMedClient))
        assert "No evidence found" in store.format_for_mcp([], "test")
        con.close()

    def test_format_for_context_xml_tags(self) -> None:
        con = duckdb.connect(":memory:")
        store = EvidenceStore(con, client=MagicMock(spec=PubMedClient))
        output = store.format_for_context([_make_article()])
        assert "<evidence>" in output
        assert '<article pmid="12345678"' in output
        con.close()

    def test_format_for_context_empty(self) -> None:
        con = duckdb.connect(":memory:")
        store = EvidenceStore(con, client=MagicMock(spec=PubMedClient))
        assert store.format_for_context([]) == ""
        con.close()


class TestEvidenceStoreCleanup:
    def test_cleanup_expired_entries(self) -> None:
        con = duckdb.connect(":memory:")
        store = EvidenceStore(con, client=MagicMock(spec=PubMedClient))
        expired_at = datetime.now() - timedelta(days=1)
        con.execute(
            "INSERT INTO agent.evidence_cache (pmid, title, abstract, authors, journal, pub_date, pub_year, doi, publication_types, mesh_terms, evidence_level, evidence_score, search_query, fetched_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)",
            [
                "expired001",
                "Expired",
                "",
                "",
                "",
                "",
                None,
                "",
                [],
                [],
                "other",
                0.3,
                "old query",
                expired_at,
            ],
        )
        assert store.cleanup_expired() == 1
        assert (
            con.execute(
                "SELECT COUNT(*) FROM agent.evidence_cache WHERE pmid = 'expired001'"
            ).fetchone()[0]
            == 0
        )
        con.close()

    def test_cleanup_keeps_valid_entries(self) -> None:
        con = duckdb.connect(":memory:")
        mc = MagicMock(spec=PubMedClient)
        mc.search.return_value = ["12345678"]
        mc.fetch_articles.return_value = [_make_article()]
        store = EvidenceStore(con, client=mc)
        store.search("test query", max_results=1)
        assert store.cleanup_expired() == 0
        assert (
            con.execute("SELECT COUNT(*) FROM agent.evidence_cache").fetchone()[0] == 1
        )
        con.close()
