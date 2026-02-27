# Learnings & Architecture Notes

Løbende log over indsigter, beslutninger og "ville-gøre-anderledes" refleksioner.
Bruges som reference ved fremtidige projekter og PoC-præsentationer.

---

## Hvad der fungerer særligt godt i dette setup

- **YAML-drevet metalag** — zero-code for nye datakilder. Tilføj én YAML-fil, resten sker automatisk via silver_runner og gold_runner. Det er det rigtige mønster for et metadata-driven platform.
- **Catalog-baseret dev/prd separation** — ét Databricks workspace, to Unity Catalogs (`health-platform-dev` / `health-platform-prd`). Simpelt, billigt og tilstrækkeligt for PoC-skala. Enterprise-alternativet er separate workspaces, men det er overkill her.
- **CI/CD fra dag 1** — `bundle validate` på PR + auto-deploy på merge. Giver tryghed og demonstrerer enterprise-modenhed selv på et personligt projekt.
- **Medallion med klare ansvarsgrænser** — bronze er rådata, silver er standardiseret, gold er forretningslogik. Ingen blanding. Nemt at forklare og nemt at vedligeholde.
- **Serverless compute** — ingen cluster-management, ingen opstartstid, ingen pris for inaktivitet. Rigtig valg for et job der kører 3 gange om dagen.

---

## Ville gøre anderledes fra dag 1

### 1. dbt i stedet for rå SQL runners
`silver_runner.py` + SQL-filer er elegant, men **dbt** giver gratis:
- Incremental logic (`{{ config(materialized='incremental') }}`)
- Schema tests (not-null, unique, accepted-values)
- Auto-genereret dokumentation og lineage-graf
- Kører identisk på DuckDB lokalt og Databricks i cloud

Prisen er lidt mere initial setup. Næste skridt: port silver-laget til dbt models.

### 2. Service principals i stedet for PAT tokens
PAT tokens er personbundne og udløber. Fra dag 1 i enterprise:
- Opret en **Databricks service principal** til CI/CD
- Brug **OAuth M2M** (machine-to-machine) i stedet for PAT
- Gem credentials i Databricks Secrets API eller HashiCorp Vault — ikke i `~/.databrickscfg`

PAT tokens er fine til PoC, men dokumentér upgrade-stien.

### 3. Data quality som gate, ikke eftertanke
DLT expectations eller Great Expectations bør være en **hård gate** i bronze → silver:
- Dårlig data må aldrig nå silver
- Fejlede rækker rutes til `bronze.quarantine_<source>` med fejlbeskrivelse
- DQ-metrics logges til `gold.data_quality_metrics` dagligt

Nuværende setup: DQ er en TODO. Ideelt: bygget ind fra første connector.

### 4. Governance og tagging fra starten
Unity Catalog tags, PII-klassifikation og data lineage er let at tilføje løbende, men **sværere at retrofitte** på mange tabeller. Næste gang: tag første tabel fra dag 1 og hold standarden.

---

## Databricks-specifikke læringer

### Bundle root skal omfavne alle notebooks
DAB (`databricks.yml`) skal ligge i den mappe der er **fælles rod** for alle notebooks og config-filer der synkroniseres. Notebook paths i job YAML er relative til orchestration-filens placering — ikke til bundle root.

**Fejlen vi lavede:** `databricks.yml` lå i `health_environment/deployment/databricks/` — notebooks lå i `health_platform/transformation_logic/databricks/`. Bundle sync root kunne ikke nå notebooks.

**Fix:** Flyt `databricks.yml` op til `health_unified_platform/` så alt er inden for sync root.

### Serverless compute kræver ingen `job_clusters` sektion
Workspaces med kun serverless compute (typisk nye Databricks-workspaces på AWS) afviser jobs med `new_cluster` konfiguration. Fjern `job_clusters` blokken helt — Databricks vælger serverless automatisk.

### `environment_variables` på task-niveau er ikke understøttet i DAB schema
Databricks bundle validate giver "unknown field" warning på `environment_variables` direkte på en task. Brug i stedet:
- `base_parameters` i `notebook_task` (bedste løsning)
- `environments` sektion på job-niveau med `environment_key` reference

### PAT token scope
Til CI/CD og bundle deployment: vælg `all-apis` scope. Bundling bruger Workspace API, Jobs API, Clusters API og Unity Catalog API simultant.

---

## Sikkerhed — regler for dette projekt

- Tokens, passwords og secrets deles **aldrig** i chat
- GitHub Secrets sættes via `gh secret set <NAME>` interaktiv prompt (ingen `--body` flag)
- Databricks credentials i `~/.databrickscfg` (globalt, aldrig i repo)
- PII-data (genetik, blodprøver, diagnoser) klassificeres `sensitive` i UC tags

---

*Sidst opdateret: 2026-02-27*
