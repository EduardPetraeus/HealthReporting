# ADR-002: Medallion Architecture (Bronze → Silver → Gold)

## Status
Accepted

## Date
2026-02-28

## Context
The platform ingests data from multiple heterogeneous sources (Apple Health, Oura, Lifesum, future: Withings, Strava). We need a clear data transformation strategy that separates raw ingestion from business logic.

## Decision
Use a three-layer medallion architecture:
- **Bronze**: Raw ingested data, minimal transformation. Tables prefixed `stg_`. One table per source endpoint.
- **Silver**: Cleaned, typed, deduplicated entities. MERGE INTO pattern with `_ingested_at` watermark. One table per entity (not per source).
- **Gold**: Aggregated, cross-source reporting entities. Views or materialized tables. Prefixed `vw_` for views.

## Consequences
- Bronze tables always preserved as-is — no destructive transformations
- Silver MERGE script names include source prefix, but table names are entity-only (e.g., `heart_rate`, not `apple_health_heart_rate`)
- Gold layer joins across sources — depends on silver stability
- `source_system` column propagated through all layers for lineage
- `_ingested_at` is the watermark for incremental loads at every layer

## Alternatives Considered
- **Two-layer (raw + clean)**: Simpler, but loses the flexibility of having gold as a separate reporting layer
- **ELT without medallion**: No clear separation, hard to debug and validate
- **SCD2 for silver**: More accurate for slowly changing dimensions, but overkill for personal health data at current scale. Document as upgrade path when multi-user platform is built.

## Scale-Up Path
At enterprise scale: introduce Delta Live Tables (DLT) for bronze → silver with built-in expectations and quarantine tables. SCD2 for slowly changing dimensions (personal_info, body metrics).

## Agent Rule
Agents must not create silver tables that combine multiple sources into one table. Each silver table is one entity. Agents must not skip layers (e.g., reading directly from bronze in gold views).
