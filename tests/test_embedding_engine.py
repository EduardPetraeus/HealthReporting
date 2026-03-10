"""Tests for EmbeddingEngine and ensure_vss_extension."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "health_unified_platform"),
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def embedding_db():
    """In-memory DuckDB with agent schema tables for embedding tests."""
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute("CREATE SCHEMA IF NOT EXISTS agent")
    con.execute(
        """
        CREATE TABLE agent.daily_summaries (
            day DATE,
            summary_text VARCHAR,
            embedding FLOAT[384],
            sleep_score INTEGER,
            readiness_score INTEGER,
            steps INTEGER,
            has_anomaly BOOLEAN,
            anomaly_metrics VARCHAR,
            data_completeness DOUBLE,
            created_at TIMESTAMP
        )
    """
    )
    con.execute(
        """
        CREATE TABLE agent.knowledge_base (
            insight_id VARCHAR PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            insight_type VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            content VARCHAR NOT NULL,
            evidence_query VARCHAR,
            confidence DOUBLE NOT NULL,
            tags VARCHAR[],
            embedding FLOAT[384],
            is_active BOOLEAN DEFAULT true,
            superseded_by VARCHAR
        )
    """
    )
    yield con
    con.close()


def _make_mock_engine(mock_st_class):
    """Helper: configure a mock SentenceTransformer and return an EmbeddingEngine."""
    from health_platform.ai.embedding_engine import EmbeddingEngine

    mock_model = MagicMock()
    mock_st_class.return_value = mock_model
    engine = EmbeddingEngine()
    return engine, mock_model


def _fixed_vector(dim: int = 384) -> np.ndarray:
    """Return a deterministic unit vector of length *dim*."""
    v = np.ones(dim, dtype=np.float32)
    return v / np.linalg.norm(v)


# ---------------------------------------------------------------------------
# embed_text
# ---------------------------------------------------------------------------


class TestEmbedText:
    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_returns_384_dim_list(self, mock_st):
        """embed_text returns a plain list of 384 floats."""
        engine, mock_model = _make_mock_engine(mock_st)
        mock_model.encode.return_value = _fixed_vector(384)

        result = engine.embed_text("test health summary")

        assert isinstance(result, list)
        assert len(result) == 384
        assert all(isinstance(v, float) for v in result)

    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_returns_correct_values(self, mock_st):
        """embed_text values match the encoded numpy array converted to list."""
        engine, mock_model = _make_mock_engine(mock_st)
        vec = _fixed_vector(384)
        mock_model.encode.return_value = vec

        result = engine.embed_text("anything")

        assert result == vec.tolist()


# ---------------------------------------------------------------------------
# embed_texts (batch)
# ---------------------------------------------------------------------------


class TestEmbedTexts:
    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_batch_returns_correct_count(self, mock_st):
        """embed_texts returns one vector per input text."""
        engine, mock_model = _make_mock_engine(mock_st)
        texts = ["text one", "text two", "text three"]
        batch = np.stack([_fixed_vector(384) for _ in texts])
        mock_model.encode.return_value = batch

        result = engine.embed_texts(texts)

        assert len(result) == 3

    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_each_vector_has_384_dims(self, mock_st):
        """Every vector in the batch output is 384-dimensional."""
        engine, mock_model = _make_mock_engine(mock_st)
        texts = ["a", "b", "c"]
        mock_model.encode.return_value = np.stack([_fixed_vector(384) for _ in texts])

        result = engine.embed_texts(texts)

        assert all(len(v) == 384 for v in result)


# ---------------------------------------------------------------------------
# _ensure_model — ImportError when sentence_transformers absent
# ---------------------------------------------------------------------------


class TestEnsureModel:
    def test_raises_import_error_when_st_none(self):
        """_ensure_model raises ImportError if SentenceTransformer is None."""
        import health_platform.ai.embedding_engine as mod

        original = mod.SentenceTransformer
        mod.SentenceTransformer = None
        try:
            from health_platform.ai.embedding_engine import EmbeddingEngine

            engine = EmbeddingEngine()
            with pytest.raises(ImportError, match="sentence-transformers"):
                engine._ensure_model()
        finally:
            mod.SentenceTransformer = original

    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_lazy_load_only_once(self, mock_st):
        """SentenceTransformer() is constructed exactly once across multiple calls."""
        engine, mock_model = _make_mock_engine(mock_st)
        mock_model.encode.return_value = _fixed_vector(384)

        engine.embed_text("first call")
        engine.embed_text("second call")

        mock_st.assert_called_once()


# ---------------------------------------------------------------------------
# search_similar — table name validation
# ---------------------------------------------------------------------------


class TestSearchSimilar:
    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_invalid_table_name_raises_value_error(self, mock_st, embedding_db):
        """Table names failing the identifier regex raise ValueError."""
        engine, mock_model = _make_mock_engine(mock_st)
        mock_model.encode.return_value = _fixed_vector(384)

        with pytest.raises(ValueError, match="Invalid table name"):
            engine.search_similar(embedding_db, "query", "DROP TABLE foo")

    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_table_name_with_dot_accepted(self, mock_st, embedding_db):
        """Qualified names like 'agent.daily_summaries' pass the regex check."""
        engine, mock_model = _make_mock_engine(mock_st)
        mock_model.encode.return_value = _fixed_vector(384)

        # No ValueError should be raised; empty result is fine (no rows yet)
        result = engine.search_similar(embedding_db, "query", "agent.daily_summaries")
        assert isinstance(result, list)

    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_returns_results_sorted_by_similarity(self, mock_st, embedding_db):
        """search_similar returns rows ordered by descending similarity_score."""
        engine, mock_model = _make_mock_engine(mock_st)

        # Insert two rows with known embeddings
        high_vec = _fixed_vector(384).tolist()
        low_vec = (_fixed_vector(384) * -1).tolist()  # opposite direction

        embedding_db.execute(
            "INSERT INTO agent.daily_summaries (day, summary_text, embedding) VALUES (?, ?, ?)",
            ["2026-03-01", "high similarity row", high_vec],
        )
        embedding_db.execute(
            "INSERT INTO agent.daily_summaries (day, summary_text, embedding) VALUES (?, ?, ?)",
            ["2026-03-02", "low similarity row", low_vec],
        )

        # Query vector identical to high_vec → first result should be 2026-03-01
        mock_model.encode.return_value = np.array(_fixed_vector(384), dtype=np.float32)

        result = engine.search_similar(
            embedding_db, "query", "agent.daily_summaries", top_k=2
        )

        assert len(result) == 2
        assert result[0]["similarity_score"] >= result[1]["similarity_score"]

    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_empty_table_returns_empty_list(self, mock_st, embedding_db):
        """search_similar on a table with no embeddings returns []."""
        engine, mock_model = _make_mock_engine(mock_st)
        mock_model.encode.return_value = _fixed_vector(384)

        result = engine.search_similar(
            embedding_db, "anything", "agent.daily_summaries", top_k=5
        )

        assert result == []

    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_exception_returns_empty_list(self, mock_st, embedding_db):
        """search_similar on a non-existent table returns [] instead of raising."""
        engine, mock_model = _make_mock_engine(mock_st)
        mock_model.encode.return_value = _fixed_vector(384)

        result = engine.search_similar(
            embedding_db, "query", "agent.nonexistent_table", top_k=5
        )

        assert result == []


# ---------------------------------------------------------------------------
# backfill_daily_summaries
# ---------------------------------------------------------------------------


class TestBackfillDailySummaries:
    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_updates_null_embeddings(self, mock_st, embedding_db):
        """backfill_daily_summaries returns the number of rows updated."""
        engine, mock_model = _make_mock_engine(mock_st)
        mock_model.encode.return_value = np.stack([_fixed_vector(384)] * 2)

        embedding_db.execute(
            "INSERT INTO agent.daily_summaries (day, summary_text, embedding) VALUES (?, ?, NULL)",
            ["2026-03-01", "summary one"],
        )
        embedding_db.execute(
            "INSERT INTO agent.daily_summaries (day, summary_text, embedding) VALUES (?, ?, NULL)",
            ["2026-03-02", "summary two"],
        )

        count = engine.backfill_daily_summaries(embedding_db)

        assert count == 2

    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_no_rows_to_update_returns_zero(self, mock_st, embedding_db):
        """backfill_daily_summaries returns 0 when all rows already have embeddings."""
        engine, mock_model = _make_mock_engine(mock_st)

        vec = _fixed_vector(384).tolist()
        embedding_db.execute(
            "INSERT INTO agent.daily_summaries (day, summary_text, embedding) VALUES (?, ?, ?)",
            ["2026-03-01", "already embedded", vec],
        )

        count = engine.backfill_daily_summaries(embedding_db)

        assert count == 0

    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_missing_table_returns_zero(self, mock_st):
        """backfill_daily_summaries returns 0 when the table does not exist."""
        import duckdb

        engine, _ = _make_mock_engine(mock_st)
        empty_con = duckdb.connect(":memory:")

        count = engine.backfill_daily_summaries(empty_con)

        assert count == 0
        empty_con.close()


# ---------------------------------------------------------------------------
# backfill_knowledge_base
# ---------------------------------------------------------------------------


class TestBackfillKnowledgeBase:
    @patch("health_platform.ai.embedding_engine.SentenceTransformer")
    def test_updates_null_embeddings(self, mock_st, embedding_db):
        """backfill_knowledge_base returns the number of rows updated."""
        engine, mock_model = _make_mock_engine(mock_st)
        mock_model.encode.return_value = np.stack([_fixed_vector(384)] * 2)

        embedding_db.execute(
            """
            INSERT INTO agent.knowledge_base
                (insight_id, insight_type, title, content, confidence)
            VALUES (?, ?, ?, ?, ?)
            """,
            ["id-001", "pattern", "Title A", "Content A", 0.9],
        )
        embedding_db.execute(
            """
            INSERT INTO agent.knowledge_base
                (insight_id, insight_type, title, content, confidence)
            VALUES (?, ?, ?, ?, ?)
            """,
            ["id-002", "pattern", "Title B", "Content B", 0.8],
        )

        count = engine.backfill_knowledge_base(embedding_db)

        assert count == 2


# ---------------------------------------------------------------------------
# ensure_vss_extension
# ---------------------------------------------------------------------------


class TestEnsureVssExtension:
    def test_returns_true_on_success(self):
        """ensure_vss_extension returns True when both INSTALL and LOAD succeed."""
        from health_platform.ai.embedding_engine import ensure_vss_extension

        mock_con = MagicMock()
        result = ensure_vss_extension(mock_con)

        assert result is True
        assert mock_con.execute.call_count == 2

    def test_returns_false_on_exception(self):
        """ensure_vss_extension returns False when execute raises."""
        from health_platform.ai.embedding_engine import ensure_vss_extension

        mock_con = MagicMock()
        mock_con.execute.side_effect = Exception("extension not found")

        result = ensure_vss_extension(mock_con)

        assert result is False
