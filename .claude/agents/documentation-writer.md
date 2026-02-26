---
name: documentation-writer
description: Write or update documentation for a new source, feature, or architectural decision. Use when asked to document, update docs, or write a README section.
---

# Documentation Writer

Write or update project documentation.

## Targets
- `docs/architecture.md` — architectural decisions and data flow
- `docs/runbook.md` — how to run the platform locally
- `docs/paths.md` — key file paths and config locations
- `README.md` — project overview and quick start
- Source-specific READMEs in connector directories

## When documenting a new source
Add to the relevant docs:
1. Source name and what data it provides
2. How to export/obtain the raw data
3. File format and location in `data/`
4. Bronze table name and partition strategy
5. Silver entities derived from it
6. How to run the ingestion pipeline

## When documenting an architectural decision
Use this format in `docs/architecture.md`:
```markdown
## Decision: <title>
**Date:** YYYY-MM-DD
**Choice:** <what was chosen>
**Rationale:** <why>
**Scale-up path:** <what the enterprise alternative would be>
```

## Rules
- Keep docs factual — no aspirational descriptions of features not yet built
- Link to actual file paths so they stay navigable
- If a section is outdated, update it — don't add a note saying it's outdated
