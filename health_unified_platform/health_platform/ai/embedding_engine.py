"""Compute and store vector embeddings for health data text.

Uses sentence-transformers (all-MiniLM-L6-v2) to embed daily summaries
and knowledge base entries. Supports vector similarity search via DuckDB
VSS extension.

sentence-transformers is an optional dependency — the module degrades
gracefully if it is not installed.
"""

from __future__ import annotations

import re

from health_platform.utils.logging_config import get_logger

logger = get_logger("embedding_engine")

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    SentenceTransformer = None

try:
    import numpy as np
except ImportError:
    np = None


# ---------------------------------------------------------------------------
# VSS extension helper
# ---------------------------------------------------------------------------


def ensure_vss_extension(con) -> bool:
    """Install and load the DuckDB VSS (vector similarity search) extension.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Open DuckDB connection.

    Returns
    -------
    bool
        True if VSS is loaded successfully, False otherwise.
    """
    try:
        con.execute("INSTALL vss")
        con.execute("LOAD vss")
        logger.info("DuckDB VSS extension loaded")
        return True
    except Exception as exc:
        logger.warning("Could not load VSS extension: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Embedding engine
# ---------------------------------------------------------------------------


class EmbeddingEngine:
    """Compute and manage vector embeddings for health data text.

    Parameters
    ----------
    model_name : str
        Name of the sentence-transformers model to use.
        Default: ``all-MiniLM-L6-v2`` (384-dimensional output).
    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model_name = model_name
        self._model = None

    def _ensure_model(self) -> None:
        """Lazy-load the sentence-transformers model on first use."""
        if self._model is not None:
            return
        if SentenceTransformer is None:
            raise ImportError(
                "sentence-transformers is required for embedding computation. "
                "Install it with: pip install sentence-transformers"
            )
        logger.info("Loading sentence-transformers model: %s", self.model_name)
        self._model = SentenceTransformer(self.model_name)
        logger.info("Model loaded successfully")

    def embed_text(self, text: str) -> list[float]:
        """Compute the embedding vector for a single text string.

        Parameters
        ----------
        text : str
            Input text to embed.

        Returns
        -------
        list[float]
            384-dimensional embedding vector.
        """
        self._ensure_model()
        vector = self._model.encode(text, convert_to_numpy=True)
        return vector.tolist()

    def embed_texts(self, texts: list[str]) -> list[list[float]]:
        """Compute embedding vectors for a batch of texts.

        Parameters
        ----------
        texts : list[str]
            Input texts to embed.

        Returns
        -------
        list[list[float]]
            List of 384-dimensional embedding vectors.
        """
        self._ensure_model()
        vectors = self._model.encode(texts, convert_to_numpy=True)
        return [v.tolist() for v in vectors]

    def backfill_daily_summaries(self, con) -> int:
        """Find daily summaries with NULL embeddings and populate them.

        Parameters
        ----------
        con : duckdb.DuckDBPyConnection
            Open DuckDB connection with agent schema available.

        Returns
        -------
        int
            Number of rows updated.
        """
        try:
            rows = con.execute(
                """
                SELECT day, summary_text
                FROM agent.daily_summaries
                WHERE embedding IS NULL
                  AND summary_text IS NOT NULL
                ORDER BY day
            """
            ).fetchall()
        except Exception as exc:
            logger.error("Failed to query daily_summaries for backfill: %s", exc)
            return 0

        if not rows:
            logger.info("No daily summaries need embedding backfill")
            return 0

        logger.info("Backfilling embeddings for %d daily summaries", len(rows))

        texts = [row[1] for row in rows]
        embeddings = self.embed_texts(texts)

        count = 0
        for (day, _), embedding in zip(rows, embeddings):
            try:
                con.execute(
                    """
                    UPDATE agent.daily_summaries
                    SET embedding = ?
                    WHERE day = ?
                """,
                    [embedding, day],
                )
                count += 1
            except Exception as exc:
                logger.error("Failed to update embedding for %s: %s", day, exc)

        logger.info("Backfilled %d / %d daily summary embeddings", count, len(rows))
        return count

    def backfill_knowledge_base(self, con) -> int:
        """Find knowledge base entries with NULL embeddings and populate them.

        Parameters
        ----------
        con : duckdb.DuckDBPyConnection
            Open DuckDB connection with agent schema available.

        Returns
        -------
        int
            Number of rows updated.
        """
        try:
            rows = con.execute(
                """
                SELECT insight_id, content
                FROM agent.knowledge_base
                WHERE embedding IS NULL
                  AND content IS NOT NULL
                ORDER BY insight_id
            """
            ).fetchall()
        except Exception as exc:
            logger.error("Failed to query knowledge_base for backfill: %s", exc)
            return 0

        if not rows:
            logger.info("No knowledge base entries need embedding backfill")
            return 0

        logger.info("Backfilling embeddings for %d knowledge base entries", len(rows))

        texts = [row[1] for row in rows]
        embeddings = self.embed_texts(texts)

        count = 0
        for (entry_id, _), embedding in zip(rows, embeddings):
            try:
                con.execute(
                    """
                    UPDATE agent.knowledge_base
                    SET embedding = ?
                    WHERE insight_id = ?
                """,
                    [embedding, entry_id],
                )
                count += 1
            except Exception as exc:
                logger.error(
                    "Failed to update embedding for knowledge_base id %s: %s",
                    entry_id,
                    exc,
                )

        logger.info("Backfilled %d / %d knowledge base embeddings", count, len(rows))
        return count

    def search_similar(
        self,
        con,
        query: str,
        table: str,
        top_k: int = 5,
    ) -> list[dict]:
        """Find the most similar entries to a query string using vector search.

        Uses cosine similarity computed via DuckDB list functions. If the VSS
        extension is available, it will be used for accelerated search.

        Parameters
        ----------
        con : duckdb.DuckDBPyConnection
            Open DuckDB connection.
        query : str
            The search query text.
        table : str
            Fully qualified table name (e.g. ``agent.daily_summaries``).
        top_k : int
            Number of results to return.

        Returns
        -------
        list[dict]
            List of dicts with keys from the table plus ``similarity_score``.
        """
        query_embedding = self.embed_text(query)

        # Validate table name to prevent SQL injection
        _safe_id = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")
        if not _safe_id.match(table):
            raise ValueError(f"Invalid table name: {table!r}")

        # Use manual cosine similarity via DuckDB list functions
        # cosine_sim = dot(a, b) / (norm(a) * norm(b))
        sql = f"""
            WITH query_vec AS (
                SELECT ?::FLOAT[] AS qvec
            )
            SELECT t.*,
                list_cosine_similarity(t.embedding, q.qvec) AS similarity_score
            FROM {table} t, query_vec q
            WHERE t.embedding IS NOT NULL
            ORDER BY similarity_score DESC
            LIMIT ?
        """
        try:
            result = con.execute(sql, [query_embedding, top_k])
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as exc:
            logger.error("Vector search failed on %s: %s", table, exc)
            return []
