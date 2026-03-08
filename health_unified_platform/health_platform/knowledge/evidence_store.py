"""Evidence cache and retrieval layer.

Sits between PubMedClient and MCP tools / chat engine.
Cache-first strategy: checks DuckDB evidence_cache table before calling PubMed API.
Article TTL: 90 days (medical literature metadata is stable).
"""

from __future__ import annotations

from datetime import datetime, timedelta

import duckdb

from health_platform.knowledge.pubmed_client import PubMedClient
from health_platform.utils.logging_config import get_logger

logger = get_logger("knowledge.evidence_store")

CACHE_TTL_DAYS = 90


class EvidenceStore:
    """Cache-first evidence retrieval from PubMed."""

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        client: PubMedClient | None = None,
    ) -> None:
        self.con = con
        self.client = client or PubMedClient()
        self._ensure_tables()

    def search(
        self,
        query: str,
        max_results: int = 5,
        min_year: str = "",
    ) -> list[dict]:
        """Search for evidence articles, cache-first."""
        normalized = self._normalize_query(query)

        cached = self._get_cached(normalized, max_results)
        if len(cached) >= max_results:
            logger.debug("Cache hit for query=%r (%d results)", normalized, len(cached))
            return cached[:max_results]

        try:
            pmids = self.client.search(
                query, max_results=max_results * 2, min_year=min_year
            )
            if not pmids:
                logger.debug("No PubMed results for query=%r", query)
                return cached

            articles = self.client.fetch_articles(pmids)
            if articles:
                self._upsert_articles(articles, normalized)
                logger.info(
                    "Fetched %d articles from PubMed for query=%r",
                    len(articles),
                    normalized,
                )
        except Exception:
            logger.warning("PubMed fetch failed for query=%r", query, exc_info=True)
            return cached

        return self._get_cached(normalized, max_results)

    def get_article(self, pmid: str) -> dict | None:
        """Get a single article by PMID from cache."""
        try:
            row = self.con.execute(
                "SELECT * FROM agent.evidence_cache WHERE pmid = ?",
                [pmid],
            ).fetchone()
        except Exception:
            return None

        if row is None:
            return None

        columns = [
            "pmid",
            "title",
            "abstract",
            "authors",
            "journal",
            "pub_date",
            "pub_year",
            "doi",
            "publication_types",
            "mesh_terms",
            "evidence_level",
            "evidence_score",
            "search_query",
            "fetched_at",
            "expires_at",
        ]
        return dict(zip(columns, row))

    def format_for_mcp(self, articles: list[dict], query: str) -> str:
        """Format articles as markdown for MCP tool output."""
        if not articles:
            return f"No evidence found for: {query}"

        parts = [f"## PubMed Evidence: {query}\n"]
        parts.append(f"*{len(articles)} articles found, sorted by evidence quality*\n")

        for i, a in enumerate(articles, 1):
            level = a.get("evidence_level", "other").replace("_", " ").title()
            score = a.get("evidence_score", 0.0)
            abstract = a.get("abstract", "")
            snippet = (abstract[:200] + "...") if len(abstract) > 200 else abstract

            parts.append(f"### {i}. {a.get('title', 'Untitled')}")
            parts.append(f"- **PMID:** {a.get('pmid', '')}")
            parts.append(f"- **Authors:** {a.get('authors', 'Unknown')}")
            parts.append(
                f"- **Journal:** {a.get('journal', '')} ({a.get('pub_date', '')})"
            )
            if a.get("doi"):
                parts.append(f"- **DOI:** {a['doi']}")
            parts.append(f"- **Evidence:** {level} (score: {score:.2f})")
            if snippet:
                parts.append(f"- **Abstract:** {snippet}")
            parts.append("")

        return "\n".join(parts)

    def format_for_context(self, articles: list[dict]) -> str:
        """Format articles as XML tags for chat engine context injection."""
        if not articles:
            return ""

        parts = ["<evidence>"]
        for a in articles[:5]:
            parts.append(
                f'  <article pmid="{a.get("pmid", "")}" '
                f'level="{a.get("evidence_level", "other")}" '
                f'score="{a.get("evidence_score", 0.0):.2f}">'
            )
            parts.append(f"    <title>{a.get('title', '')}</title>")
            parts.append(
                f"    <citation>{a.get('authors', '')}. "
                f"{a.get('journal', '')}. {a.get('pub_date', '')}.</citation>"
            )
            abstract = a.get("abstract", "")
            if abstract:
                parts.append(f"    <abstract>{abstract[:300]}</abstract>")
            parts.append("  </article>")
        parts.append("</evidence>")

        return "\n".join(parts)

    def cleanup_expired(self) -> int:
        """Delete expired cache entries. Returns count of deleted rows."""
        try:
            before = self.con.execute(
                "SELECT COUNT(*) FROM agent.evidence_cache "
                "WHERE expires_at < CURRENT_TIMESTAMP"
            ).fetchone()[0]
            if before > 0:
                self.con.execute(
                    "DELETE FROM agent.evidence_cache "
                    "WHERE expires_at < CURRENT_TIMESTAMP"
                )
                logger.info("Cleaned up %d expired evidence cache entries", before)
            return before
        except Exception:
            logger.debug("Cleanup failed", exc_info=True)
            return 0

    def _ensure_tables(self) -> None:
        """Create evidence_cache table if it does not exist."""
        self.con.execute("CREATE SCHEMA IF NOT EXISTS agent")
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS agent.evidence_cache (
                pmid              VARCHAR PRIMARY KEY,
                title             VARCHAR NOT NULL,
                abstract          VARCHAR,
                authors           VARCHAR,
                journal           VARCHAR,
                pub_date          VARCHAR,
                pub_year          INTEGER,
                doi               VARCHAR,
                publication_types VARCHAR[],
                mesh_terms        VARCHAR[],
                evidence_level    VARCHAR,
                evidence_score    DOUBLE,
                search_query      VARCHAR NOT NULL,
                fetched_at        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                expires_at        TIMESTAMP NOT NULL
            )
        """
        )

    def _get_cached(self, normalized_query: str, max_results: int) -> list[dict]:
        """Get cached articles matching query with valid TTL."""
        try:
            rows = self.con.execute(
                """
                SELECT pmid, title, abstract, authors, journal,
                       pub_date, pub_year, doi, publication_types, mesh_terms,
                       evidence_level, evidence_score, search_query,
                       fetched_at, expires_at
                FROM agent.evidence_cache
                WHERE search_query = ?
                  AND expires_at > CURRENT_TIMESTAMP
                ORDER BY evidence_score DESC
                LIMIT ?
                """,
                [normalized_query, max_results],
            ).fetchall()
        except Exception:
            return []

        columns = [
            "pmid",
            "title",
            "abstract",
            "authors",
            "journal",
            "pub_date",
            "pub_year",
            "doi",
            "publication_types",
            "mesh_terms",
            "evidence_level",
            "evidence_score",
            "search_query",
            "fetched_at",
            "expires_at",
        ]
        return [dict(zip(columns, row)) for row in rows]

    def _upsert_articles(self, articles: list[dict], search_query: str) -> None:
        """Insert or update articles in the cache."""
        expires_at = datetime.now() + timedelta(days=CACHE_TTL_DAYS)

        for a in articles:
            try:
                self.con.execute(
                    """
                    INSERT OR REPLACE INTO agent.evidence_cache
                        (pmid, title, abstract, authors, journal,
                         pub_date, pub_year, doi, publication_types, mesh_terms,
                         evidence_level, evidence_score, search_query,
                         fetched_at, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                    """,
                    [
                        a["pmid"],
                        a["title"],
                        a.get("abstract", ""),
                        a.get("authors", ""),
                        a.get("journal", ""),
                        a.get("pub_date", ""),
                        a.get("pub_year"),
                        a.get("doi", ""),
                        a.get("publication_types", []),
                        a.get("mesh_terms", []),
                        a.get("evidence_level", "other"),
                        a.get("evidence_score", 0.3),
                        search_query,
                        expires_at,
                    ],
                )
            except Exception:
                logger.debug("Failed to upsert PMID %s", a.get("pmid"), exc_info=True)

    @staticmethod
    def _normalize_query(query: str) -> str:
        """Normalize search query for cache matching."""
        return " ".join(query.lower().split())
