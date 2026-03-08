# C4 Architecture — HealthReporting

> Solution architecture narrative + C4 diagrams (Level 1–3).
> For detailed tables, file structure, and technology stack, see [ARCHITECTURE.md](./ARCHITECTURE.md).
> For an interactive view, see [architecture-diagram.html](./architecture-diagram.html).

---

## Solution Architecture Narrative

### Problem

Health data lives in silos. Oura knows sleep. Apple Health knows steps and heart rate. Lifesum knows nutrition. No single system connects them — and no system is designed for an AI agent to query.

Traditional data platforms solve this with a medallion architecture: Bronze (raw), Silver (clean), Gold (aggregated views for dashboards). But in this platform, the primary local consumer is not a human looking at dashboards — it is Claude Code, an AI agent querying data through MCP tools.

### The Dual-Stack Decision

The platform uses a **dual-stack architecture** where Silver is the shared layer and divergence happens after Silver:

- **Local (Mac Mini M4):** Silver feeds into an AI-native 2+2 model — Agent Memory (embeddings, knowledge graph, patient profile) and Semantic Contracts (YAML metric definitions exposed via 8 MCP tools). There is no Gold layer locally. The AI computes any aggregation on-demand in <100ms via DuckDB.
- **Cloud (Databricks):** Silver feeds into traditional Gold views for BI dashboards and enterprise consumers. This preserves the PoC value of demonstrating a standard medallion pattern.

This split exists because Gold encodes assumptions about what questions will be asked. An AI agent generates queries on-demand for any question — pre-aggregated views hide the detail it needs. At N=1 scale, materializing Gold locally is pure overhead. See [ADR-005](./adr/ADR-005-ai-native-data-model.md) for the full rationale.

### Key Properties

1. **Metadata-driven** — adding a new data source requires a YAML config entry, not code changes ([ADR-003](./adr/ADR-003-yaml-driven-pipeline.md))
2. **MCP-first** — AI never writes raw SQL. All data access goes through 8 typed MCP tools with semantic contracts
3. **Schema-as-documentation** — every column across 21 silver tables has a `COMMENT ON` description. The schema IS the AI's user interface (83% query accuracy vs 40% without)
4. **DuckDB local-first** — zero-config OLAP runtime, Parquet interchange format, single-writer simplicity ([ADR-001](./adr/ADR-001-duckdb-local-runtime.md))
5. **Medallion foundation** — Bronze → Silver shared across both stacks, source isolation via `source_system` column ([ADR-002](./adr/ADR-002-medallion-architecture.md))

---

## C4 Level 1: System Context

Who interacts with the platform, and what external systems does it depend on?

```mermaid
C4Context
    title System Context — HealthReporting Platform

    Person(claus, "Claus", "Developer and data owner")
    Person(claude, "Claude Code", "AI agent — primary local data consumer")

    System(health, "HealthReporting Platform", "Dual-stack health data platform: AI-native locally, traditional medallion in cloud")

    System_Ext(oura, "Oura API", "Sleep, activity, readiness, HR, SpO2, stress")
    System_Ext(apple, "Apple Health", "HR, steps, temperature, respiratory rate, energy, gait, mindfulness, water, toothbrushing")
    System_Ext(lifesum, "Lifesum", "Nutrition and meal tracking")
    System_Ext(withings, "Withings API", "Blood pressure, weight (planned)")
    System_Ext(strava, "Strava API", "Workout activities (planned)")
    System_Ext(gettested, "GetTested", "Blood test biomarkers (planned)")

    System_Ext(databricks, "Databricks", "Unity Catalog, Delta Lake, Workflows, AI/BI Dashboards")
    System_Ext(github, "GitHub", "Source control, Actions CI/CD, Projects board")
    System_Ext(tailscale, "Tailscale", "VPN mesh for remote API access")
    System_Ext(ntfy, "ntfy.sh", "Push notifications for pipeline status")

    Rel(claus, health, "Manages, configures, reviews")
    Rel(claude, health, "Queries health data via MCP tools and REST API")
    Rel(health, oura, "Fetches data via OAuth 2.0")
    Rel(health, apple, "Imports XML export")
    Rel(health, lifesum, "Imports CSV export")
    Rel(health, withings, "Fetches data via API (planned)")
    Rel(health, strava, "Fetches data via API (planned)")
    Rel(health, gettested, "Imports results (planned)")
    Rel(health, databricks, "Deploys bundles, runs cloud pipelines")
    Rel(health, github, "CI/CD, PR validation, issue tracking")
    Rel(health, tailscale, "Exposes API over VPN")
    Rel(health, ntfy, "Sends pipeline notifications")
```

---

## C4 Level 2: Container Diagram

What are the major containers (deployable units) in each stack?

```mermaid
C4Container
    title Container Diagram — HealthReporting Platform

    Person(claude, "Claude Code", "AI agent")
    Person(claus, "Claus", "Developer")

    System_Ext(oura, "Oura API", "")
    System_Ext(apple, "Apple Health", "")
    System_Ext(lifesum, "Lifesum", "")

    Boundary(local, "Local Stack (Mac Mini M4)") {
        Container(connectors, "Source Connectors", "Python", "Oura OAuth client, Apple Health XML parser, CSV-to-Parquet converter")
        Container(ingestion, "Ingestion Engine", "Python", "Reads sources_config.yaml, loads Parquet into Bronze tables")
        Container(dbt_merge, "dbt + Merge Runner", "Python/SQL", "Schema creation (dbt) + MERGE INTO (SQL scripts)")
        Container(ai_modules, "AI Modules", "Python", "Text generation, embeddings, baselines, correlations")
        Container(agent_memory, "Agent Memory", "DuckDB schema", "patient_profile, daily_summaries, health_graph, knowledge_base")
        Container(contracts, "Semantic Contracts", "YAML", "18 metric definitions + business rules + master index")
        Container(mcp, "MCP Server", "Python/FastMCP", "8 typed tools — primary AI data access layer")
        Container(api, "FastAPI REST Server", "Python", "5 endpoints, Bearer auth, Tailscale-accessible")
        Container(sync, "Daily Sync", "Bash/launchd", "Oura fetch → Bronze → Silver → Summary at 06:00")
        ContainerDb(duckdb, "DuckDB Database", "DuckDB", "bronze, silver, agent schemas")
        ContainerDb(parquet, "Parquet Data Lake", "Hive-partitioned files", "Raw source data interchange")
    }

    Boundary(cloud, "Cloud Stack (Databricks)") {
        Container(autoloader, "Autoloader", "PySpark", "Ingests Parquet from cloud storage into Bronze Delta tables")
        Container(silver_runner, "Silver Runner", "SQL", "10 silver transforms deployed via DAB")
        Container(gold_runner, "Gold Runner", "SQL", "3 reporting views")
        ContainerDb(unity, "Unity Catalog", "Delta Lake", "health-platform-dev / health-platform-prd catalogs")
        Container(workflows, "Scheduled Workflows", "DAB", "Bronze, Silver, Gold orchestration jobs")
        Container(cicd, "GitHub Actions CI/CD", "YAML", "Bundle validation on PR, auto-deploy dev and prd")
    }

    Rel(claus, api, "Manages via REST")
    Rel(claude, mcp, "Queries via MCP tools")
    Rel(claude, api, "Queries via REST API")

    Rel(oura, connectors, "OAuth 2.0 API")
    Rel(apple, connectors, "XML export")
    Rel(lifesum, connectors, "CSV export")

    Rel(connectors, parquet, "Writes Parquet files")
    Rel(parquet, ingestion, "Reads Parquet")
    Rel(ingestion, duckdb, "Loads Bronze tables")
    Rel(duckdb, dbt_merge, "Bronze → Silver")
    Rel(dbt_merge, duckdb, "Writes Silver tables")
    Rel(duckdb, ai_modules, "Reads Silver data")
    Rel(ai_modules, duckdb, "Writes Agent Memory")
    Rel(contracts, mcp, "Defines query semantics")
    Rel(duckdb, mcp, "Executes queries")
    Rel(agent_memory, mcp, "Provides memory context")
    Rel(sync, connectors, "Triggers daily fetch")
    Rel(sync, ingestion, "Triggers ingestion")
    Rel(sync, dbt_merge, "Triggers merge")
    Rel(sync, ai_modules, "Triggers summary generation")

    Rel(parquet, autoloader, "Cloud copy of Parquet")
    Rel(autoloader, unity, "Writes Bronze Delta")
    Rel(unity, silver_runner, "Bronze → Silver")
    Rel(silver_runner, unity, "Writes Silver Delta")
    Rel(unity, gold_runner, "Silver → Gold")
    Rel(gold_runner, unity, "Writes Gold views")
    Rel(workflows, autoloader, "Orchestrates")
    Rel(workflows, silver_runner, "Orchestrates")
    Rel(workflows, gold_runner, "Orchestrates")
    Rel(cicd, workflows, "Deploys DAB bundles")
```

---

## C4 Level 3: Component Diagram — AI-Native Local Stack

Zooming into the MCP Server, AI Modules, Agent Memory, and Semantic Contracts — the components that make the local stack AI-native.

```mermaid
C4Component
    title Component Diagram — AI-Native Data Access Layer

    Person(claude, "Claude Code", "AI agent")

    Boundary(mcp_boundary, "MCP Server (FastMCP)") {
        Component(server, "server.py", "FastMCP", "Server entrypoint — registers 8 tools")
        Component(health_tools, "health_tools.py", "Python", "Tool implementations — query routing, memory ops, insight recording")
        Component(query_builder, "query_builder.py", "Python", "YAML contract → parameterized SQL generation")
        Component(formatter, "formatter.py", "Python", "Query results → markdown output formatting")
        Component(schema_pruner, "schema_pruner.py", "Python", "Category-based schema pruning — only relevant tables in context")
    }

    Boundary(ai_boundary, "AI Modules") {
        Component(text_gen, "text_generator.py", "Python", "Template-based daily health narrative generation")
        Component(embeddings, "embedding_engine.py", "Python", "sentence-transformers (all-MiniLM-L6-v2) — 384-dim embeddings + vector search")
        Component(baselines, "baseline_computer.py", "Python", "Rolling baselines + demographics → patient_profile entries")
        Component(correlations, "correlation_engine.py", "Python", "Pearson correlations with lag detection → metric_relationships")
    }

    Boundary(contract_boundary, "Semantic Contracts (YAML)") {
        Component(index_yml, "_index.yml", "YAML", "Master index — 9 categories, query routing config, schema pruning rules")
        Component(rules_yml, "_business_rules.yml", "YAML", "Composite health score (35/35/30 weighting), 5 alert thresholds, anomaly detection")
        Component(metric_ymls, "18 metric YAMLs", "YAML", "Per-metric: computation SQL, thresholds, baselines, related metrics, example queries")
    }

    Boundary(memory_boundary, "Agent Memory (DuckDB agent schema)") {
        ComponentDb(profile, "patient_profile", "Core memory", "Demographics + baselines — always in context (~2000 tokens)")
        ComponentDb(summaries, "daily_summaries", "Recall memory", "One row per day + 384-dim embeddings for vector search")
        ComponentDb(graph, "health_graph + edges", "Relationship memory", "67 nodes, 108 edges — semantic knowledge graph")
        ComponentDb(knowledge, "knowledge_base", "Archival memory", "Accumulated insights — vector-searchable, grows over time")
    }

    Rel(claude, server, "MCP protocol")
    Rel(server, health_tools, "Dispatches tool calls")
    Rel(health_tools, query_builder, "Builds SQL from YAML contracts")
    Rel(health_tools, formatter, "Formats results")
    Rel(health_tools, schema_pruner, "Selects relevant schema subset")
    Rel(query_builder, index_yml, "Reads routing config")
    Rel(query_builder, metric_ymls, "Reads computation SQL")
    Rel(schema_pruner, index_yml, "Reads pruning rules")
    Rel(health_tools, profile, "get_profile — loads core memory")
    Rel(health_tools, summaries, "search_memory — vector search")
    Rel(health_tools, graph, "discover_correlations — traverses graph")
    Rel(health_tools, knowledge, "record_insight / search_memory")
    Rel(text_gen, summaries, "Generates daily narratives")
    Rel(embeddings, summaries, "Computes and stores embeddings")
    Rel(baselines, profile, "Computes and updates baselines")
    Rel(correlations, graph, "Writes correlation edges")
    Rel(rules_yml, health_tools, "Defines alert logic and scoring")
```

---

## Maintenance Notes

- **When to update these diagrams:** When a new data source connector is added, a new MCP tool is created, or the dual-stack boundary changes (e.g., new cloud services or local components).
- **For detailed tables** (bronze/silver/gold contents, file paths, technology stack): see [ARCHITECTURE.md](./ARCHITECTURE.md).
- **For interactive exploration:** see [architecture-diagram.html](./architecture-diagram.html).
- **Rendering:** GitHub renders Mermaid natively. For complex diagrams, use the [Mermaid Live Editor](https://mermaid.live) if nested boundaries render unexpectedly.
- **C4 plugin:** These diagrams use Mermaid's C4 extension (`C4Context`, `C4Container`, `C4Component`). Ensure your viewer supports C4 syntax.
