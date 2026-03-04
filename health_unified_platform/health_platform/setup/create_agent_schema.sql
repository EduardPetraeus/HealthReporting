-- Agent Memory Schema for AI-Native Health Data Model
-- DuckDB-compatible DDL
--
-- Memory architecture follows the MemGPT pattern:
--   CORE MEMORY     — always loaded into agent context (~2000 tokens)
--   RECALL MEMORY   — structured daily snapshots with embeddings
--   RELATIONSHIP     — semantic knowledge graph of health domain
--   ARCHIVAL MEMORY — accumulated insights, vector-searchable

CREATE SCHEMA IF NOT EXISTS agent;

-- =============================================================================
-- CORE MEMORY: always loaded into context (~2000 tokens)
-- Key-value store for patient profile facts that the agent references every turn.
-- =============================================================================
CREATE TABLE IF NOT EXISTS agent.patient_profile (
    profile_key         VARCHAR PRIMARY KEY,
    profile_value       VARCHAR NOT NULL,
    numeric_value       DOUBLE,
    category            VARCHAR NOT NULL,
    description         VARCHAR NOT NULL,
    computed_from       VARCHAR,
    last_updated_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_frequency    VARCHAR
);

-- =============================================================================
-- RECALL MEMORY: one row per day, structured + embedded
-- Compact daily health snapshots that power temporal reasoning and retrieval.
-- =============================================================================
CREATE TABLE IF NOT EXISTS agent.daily_summaries (
    day                 DATE PRIMARY KEY,
    sleep_score         INTEGER,
    readiness_score     INTEGER,
    steps               INTEGER,
    resting_hr          DOUBLE,
    stress_level        VARCHAR,
    has_anomaly         BOOLEAN DEFAULT FALSE,
    anomaly_metrics     VARCHAR,
    summary_text        VARCHAR NOT NULL,
    embedding           FLOAT[384],
    data_completeness   DOUBLE,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- RELATIONSHIP MEMORY: semantic knowledge graph
-- Nodes and edges encoding health domain knowledge, metric relationships,
-- and causal/correlational links between biomarkers, activities, and conditions.
-- =============================================================================
CREATE TABLE IF NOT EXISTS agent.health_graph (
    node_id             VARCHAR PRIMARY KEY,
    node_type           VARCHAR NOT NULL,
    node_label          VARCHAR NOT NULL,
    description         VARCHAR,
    related_tables      VARCHAR,
    related_columns     VARCHAR
);

CREATE TABLE IF NOT EXISTS agent.health_graph_edges (
    source_node_id      VARCHAR NOT NULL,
    target_node_id      VARCHAR NOT NULL,
    edge_type           VARCHAR NOT NULL,
    weight              DOUBLE,
    evidence            VARCHAR,
    description         VARCHAR,
    PRIMARY KEY (source_node_id, target_node_id, edge_type)
);

-- =============================================================================
-- ARCHIVAL MEMORY: accumulated insights, vector-searchable
-- Long-term knowledge base where the agent stores derived insights,
-- patterns, and recommendations discovered over time.
-- =============================================================================
CREATE TABLE IF NOT EXISTS agent.knowledge_base (
    insight_id          VARCHAR PRIMARY KEY,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    insight_type        VARCHAR NOT NULL,
    title               VARCHAR NOT NULL,
    content             VARCHAR NOT NULL,
    evidence_query      VARCHAR,
    confidence          DOUBLE,
    tags                VARCHAR[],
    embedding           FLOAT[384],
    is_active           BOOLEAN DEFAULT TRUE,
    superseded_by       VARCHAR
);

-- =============================================================================
-- CROSS-LAYER: metric relationships (silver schema)
-- Quantified statistical relationships between health metrics.
-- Lives in silver because it bridges raw data and agent reasoning.
-- =============================================================================
CREATE TABLE IF NOT EXISTS silver.metric_relationships (
    source_metric       VARCHAR NOT NULL,
    target_metric       VARCHAR NOT NULL,
    relationship_type   VARCHAR NOT NULL,
    strength            DOUBLE,
    lag_days            INTEGER DEFAULT 0,
    direction           VARCHAR,
    evidence_type       VARCHAR,
    confidence          DOUBLE,
    description         VARCHAR,
    last_computed_at    TIMESTAMP,
    PRIMARY KEY (source_metric, target_metric, relationship_type, lag_days)
);

-- =============================================================================
-- Vector similarity indexes (requires DuckDB vss extension)
-- INSTALL vss; LOAD vss;
-- CREATE INDEX idx_summaries_emb ON agent.daily_summaries USING HNSW (embedding) WITH (metric = 'cosine');
-- CREATE INDEX idx_knowledge_emb ON agent.knowledge_base USING HNSW (embedding) WITH (metric = 'cosine');
-- Enable these after installing the vss extension: INSTALL vss; LOAD vss;
-- =============================================================================
