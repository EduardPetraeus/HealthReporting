# TODO

## Configuration

- [ ] **`databricks.yml` workspace URLs** — replace placeholder hosts with real Databricks workspace URLs for `dev` and `prd` targets in `health_unified_platform/health_environment/deployment/databricks/databricks.yml`
- [ ] **`.env.example`** — document required environment variables (Oura OAuth client ID/secret, Databricks host, token, catalog name, `HEALTH_ENV`)

## Databricks Framework — Complete Coverage

Work in progress. Currently only example configs exist.

- [ ] **Source YAML configs** — 2 / 27 configured in `health_environment/config/databricks/sources/`
- [ ] **Silver SQL transforms** — 1 / 18 implemented in `transformation_logic/databricks/silver/sql/`
- [ ] **Gold configs + SQL** — 1 entity in `health_environment/config/databricks/gold/` + `transformation_logic/databricks/gold/sql/`

See `README.md` files in each folder for the specific remaining items.

## Apple Health Connector

- [ ] **Workout elements** — XML contains `Workout`, `WorkoutEvent`, `WorkoutStatistics` elements that are currently ignored. Add parsing alongside `Record` elements.
- [ ] **Partition consistency** — Apple Health uses `domain/type/year=YYYY/`, Oura uses `year=YYYY/month=MM/day=DD/`. Consider aligning to one scheme.
- [ ] **End-to-end wrapper script** — single shell script that runs: XML → parquet → bronze ingestion → silver merge for all Apple Health types
- [ ] **State file** — track last ingested export date (`~/.config/health_reporting/apple_health_state.json`) to enable true incremental runs

## Features — Prioritized Backlog

### Tier 1 — Høj værdi, bygger på eksisterende infrastruktur

- [ ] **Ugentlig health digest** — automatisk Markdown-rapport hver mandag via GitHub Actions cron. Forrige uge vs ugen før på tværs af alle silver-tabeller. Ingen ny infrastruktur.
- [ ] **Anomali-detektor** — flag usædvanlige målinger (meget høj puls, meget lav søvnscore) med simpel statistik (z-score eller percentil). Ren SQL/Python.
- [ ] **Master data annoterings-app** — lokal Streamlit-app til manuel annotation af kontekst wearables ikke kan fange: sygdom, rejse, stress, ny træningsrutine. Gemmes i `gold.daily_annotations` og joines på alle gold-views.
- [ ] **Databricks AI/BI dashboard** — gold-laget eksisterer, naturligt næste skridt. Viser trends, scores og anomalier. Ingen ekstra infrastruktur når Databricks er konfigureret.

### Tier 2 — Høj værdi, mere arbejde

- [ ] **Korrelationsmotor** — automatisk find sammenhænge på tværs af metrics. "Dage med søvnscore > 80 giver X% lavere hvilepuls næste dag." Ren SQL, ingen ML.
- [ ] **Composite health score** — ét dagligt tal beregnet fra søvn + aktivitet + stress + readiness. Defineres som en gold-view med vægtede inputs.
- [ ] **Withings / Garmin connector** — flere datakilder ind i samme medallion-arkitektur. Withings: vægt, blodtryk. Garmin: avancerede træningsmetrics.
- [ ] **Familie-platform** — tilføj `user_id` dimension. Arkitekturen understøtter allerede source isolation — relativt lille refaktor at tracke flere brugeres data i samme platform.

### Tier 3 — Længere sigt

- [ ] **Forudsigelsesmodel** — baseret på gårsdagens søvn + aktivitet, forudsig dagens readiness. Simpel lineær regression. Kræver minimum 6 måneders historisk data.
- [ ] **Personal health API** — FastAPI på Mac Mini der eksponerer gold-laget som REST endpoint. Gør data tilgængeligt for andre apps og devices.
- [ ] **Lokal Streamlit-visualisering** — interaktivt health dashboard der kører på Mac Mini og læser direkte fra DuckDB. Flersidet layout: søvn / aktivitet / vitals / trends. Tilgængeligt fra telefon/iPad via lokalt netværk.
- [ ] **Databricks AI/BI dashboard** — BI-værktøj direkte på gold-laget uden ekstra infrastruktur. AI-assistent der svarer på spørgsmål om dine data på naturligt sprog. Prioritér når Databricks er konfigureret.

### Tier 4 — Passiv indkomst

- [ ] **Health data platform template** — sælg hele arkitekturen (medallion + connectors + dashboard) som Databricks-template til data engineers der vil tracke eget helbred. Udgangspunkt: dette repo poleret og generaliseret.
- [ ] **Anonymiseret benchmark** — "Din søvnscore er i top 30% for mænd 35-45." Kræver opt-in data fra andre brugere og privacy-arkitektur.

## Quality & Testing

- [ ] **dbt tests** — add `schema.yml` with not-null, unique, accepted-values tests per silver entity
- [ ] **Freshness checks** — validate last ingested timestamp per source (bronze → silver lag)
- [ ] **Row count reconciliation** — bronze vs silver row counts after each merge run

## Cleanup

- [ ] **`health_environment/deployment/databricks/`** — catalog/schema DDL scripts (`create_catalog__health_dw.sql`, `create_schemas__health_dw.sql`) may be superseded by `init.py` — consider archiving
