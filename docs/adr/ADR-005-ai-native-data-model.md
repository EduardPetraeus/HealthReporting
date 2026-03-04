# ADR-005: AI-Native Data Model (2+2 Architecture)

## Status
Accepted

## Date
2026-03-04

## Context

The HealthReporting platform originally followed a 5-layer medallion architecture (Bronze → Silver → Gold → Platinum → Diamond) designed for human consumers viewing dashboards. However, the primary local consumer is now an AI agent (Claude Code via MCP), not a human looking at BI tools.

Research across 80+ sources revealed a fundamental mismatch:

- **Gold layer encodes assumptions** about what questions will be asked, but AI generates queries on-demand for any question
- **Pre-aggregated views hide detail** the AI might need (Microsoft: "Gold removes details agents need")
- **Static materialization at N=1 scale is pointless** — DuckDB computes any aggregation in <100ms for millions of rows
- **dbt Semantic Layer** gives 83% query accuracy vs. 40% without — the schema IS the AI's user interface
- **Schema pruning** (CHESS architecture) is critical — sending full schema crashes Text2SQL accuracy (Spider 2.0: 86% → 10%)

Meanwhile, the Databricks cloud stack needs traditional Gold for BI dashboards and enterprise consumers. This creates a dual-stack requirement.

## Decision

Adopt a **2+2 architecture** for the local stack while keeping traditional medallion in the cloud:

### Local Stack (AI-Native)
```
Layer 1: Raw Store (= Bronze, unchanged)
  Append-only parquet. Source-partitioned.

Layer 2: Unified Health Store (= Enhanced Silver)
  21 deduplicated tables + COMMENT ON every column + metric_relationships graph

Layer 3: Agent Memory (new agent schema)
  Core memory (patient_profile), Recall memory (daily_summaries + embeddings),
  Relationship memory (health_graph), Archival memory (knowledge_base)

Layer 4: Semantic Contract (YAML files, not tables)
  18 metric definitions with computation SQL, thresholds, baselines
  Exposed via 8 MCP tools — AI never writes raw SQL
```

### Cloud Stack (Traditional, unchanged)
Bronze → Silver → Gold for BI dashboards. Same dbt models. Divergence happens after Silver.

### Key Architectural Choices

1. **No local Gold layer** — Semantic Contracts (YAML) define computations on-demand, replacing materialized Gold views
2. **MCP-first data access** — AI calls typed tools (`query_health`, `search_memory`, `discover_correlations`), never raw SQL
3. **MemGPT-inspired memory tiers** — Core (~2000 tokens, always loaded), Recall (per-day summaries), Relationship (knowledge graph), Archival (accumulated insights)
4. **Embedded daily narratives** — structured health rows converted to natural language summaries and embedded with sentence-transformers (all-MiniLM-L6-v2, 384-dim) for vector search
5. **Schema as documentation** — COMMENT ON every column across all 21 silver tables (~150 descriptions)

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Keep Gold layer locally | Redundant — AI can compute any aggregation on-demand. Maintenance overhead for zero benefit at N=1. |
| Use dbt Semantic Layer / Cube.dev | Requires running a semantic layer server. YAML contracts achieve the same goal with zero infrastructure. |
| Use GraphRAG (full knowledge graph DB) | Over-engineered for personal health. DuckDB tables + seed SQL sufficient. |
| LLM-based text generation | Requires API calls, adds latency and cost. Template-based generation is deterministic and fast. |
| Single-stack (drop Databricks Gold) | Enterprise PoC value requires demonstrating traditional patterns alongside AI-native ones. |

## Consequences

### Positive
- AI agent gets 83%+ query accuracy via semantic contracts vs ~40% with raw schema access
- Daily summaries enable natural language search across health history
- Knowledge graph captures domain relationships (67 nodes, 108 edges)
- Patient profile provides always-available context (~2000 tokens)
- No Gold layer maintenance — adding a new metric = adding a YAML file
- Dual-stack preserves enterprise PoC value

### Negative
- Two divergent architectures to maintain (local AI-native vs cloud medallion)
- Semantic contracts require manual authoring per metric
- Vector search requires sentence-transformers dependency (~400MB model)
- HNSW indexes require `hnsw_enable_experimental_persistence` flag (DuckDB vss is still experimental)
- No industry standard for this pattern — we are the experiment

### Risks
- DuckDB vss extension stability (experimental persistence)
- Embedding model quality for health-domain text (general-purpose model, not domain-fine-tuned)
- YAML contract drift if silver schema changes without updating contracts

## Validation

| Check | Result |
|---|---|
| 55/55 pytest tests pass | PASS |
| 91 daily summaries backfilled with embeddings | PASS |
| 9 patient profile entries computed from real data | PASS |
| 67 knowledge graph nodes, 108 edges seeded | PASS |
| Vector search returns semantically relevant results | PASS |
| HNSW index created with experimental persistence | PASS |
| All 20 YAML files parse as valid YAML | PASS |
| Column comments applied to all 21 silver tables | PASS |

## References

- Google Cloud: "System of Record to System of Reason"
- Microsoft: "Data Architecture for AI Agents"
- dbt Semantic Layer: 83% vs 40% accuracy benchmarks
- Spider 2.0: schema complexity impact on Text2SQL
- Letta/MemGPT: tiered memory architecture for agents
- TAG paper (Berkeley/Stanford): 3x improvement with table-augmented generation
- CHESS architecture: schema pruning for Text2SQL
