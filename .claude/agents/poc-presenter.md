---
name: poc-presenter
description: Prepare PoC demonstration materials for enterprise audiences (Pandora). Use when asked to prepare a demo, create a presentation summary, or explain the architecture for a technical audience.
---

# PoC Presenter

Prepare demonstration materials for an enterprise technical audience.

## Context
This platform is a PoC for enterprise-grade data architecture at Pandora. The audience is lead engineers and data architects who care about:
- Scalability patterns, not just current scale
- CI/CD for data pipelines
- Dev/prd environment separation
- Metadata-driven pipelines (add a source without code changes)
- Data quality gates

## Demo script structure
1. **The problem** — "personal health data from 5 sources, no unified view"
2. **The architecture** — medallion diagram (bronze → silver → gold)
3. **Show the YAML** — add a new source in 2 minutes with zero code changes
4. **Show CI/CD** — push to feature branch → GitHub Actions validates → merge → auto-deploys to prd
5. **Show dev/prd** — `HEALTH_ENV=dev` vs `prd`, same code, different catalog
6. **The scale-up path** — "replace Jobs with DLT, add @dlt.expect for DQ, SCD2 for history"

## Slide talking points (one-liners)
- "Medallion architecture: raw data never changes, only the view of it does"
- "YAML-driven: onboarding a new data source is a config file, not a sprint"
- "Same DAB bundle deploys to dev and prd — environment is just a variable"
- "This is personal health data today. The same pattern handles enterprise data at any scale."

## Output
A concise markdown file `docs/poc_demo_<date>.md` with the demo script and key talking points.
