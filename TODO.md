# TODO

## Prioriteret Arbejdsrækkefølge

Overordnet rækkefølge — arbejd oppefra ned. Hver blok er afhængig af dem over.

| # | Opgave | Blocker for | Sektion |
|---|--------|-------------|---------|
| ~~1~~ | ~~Databricks workspace URLs + secrets~~ | ~~Alt på Databricks~~ | ✅ Løst |
| 2 | Fuld data model (alle kilder) | Connectors + gold design | Data Model |
| 3 | P0 quick wins (logging, path-validering) | Stabil lokal pipeline | Optimering |
| 4 | Withings API connector | Withings scheduled job | Withings Connector |
| 5 | Strava API connector | Strava scheduled job | Strava Connector |
| 6 | Min Sundhed connector (sundhed.dk) | Klinisk data i platformen | Min Sundhed Connector |
| 7 | Lifesum login-agent | Daglig mad-data | Lifesum Connector |
| 8 | Apple Health auto-ingest zip-flow | Apple i scheduled flow | Apple Auto-Ingest |
| 9 | Scheduled jobs — alle kilder i Databricks | Fuld automatisering | Scheduled Jobs |
| 10 | Web app MVP (Streamlit + Cloudflare) | Dashboard fra iPhone | Web App |
| 11 | Data SLA + monitoring | Datakvalitet i prd | Data SLA |

## Configuration

- [x] **`databricks.yml` workspace URLs** — dev og prd workspace URLs konfigureret
- [x] **GitHub Secrets** — `DATABRICKS_HOST_DEV`, `DATABRICKS_TOKEN_DEV`, `DATABRICKS_HOST_PRD`, `DATABRICKS_TOKEN_PRD` sat op. CI/CD kører fuldt.
- [ ] **`.env.example`** — document required environment variables (Oura OAuth client ID/secret, Databricks host, token, catalog name, `HEALTH_ENV`)
- [x] **Re-enable branch protection status check** — `Validate bundle` aktiveret som required check på `main`.

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

- [ ] **Personal health API** — FastAPI på Mac Mini der eksponerer gold-laget som REST endpoint. Fundament for alt andet — widgets, Shortcuts, andre apps, Databricks. Ingen ny infrastruktur.
  - [ ] **Cloudflare Tunnel** — giver Mac Mini en fast public HTTPS URL uden statisk IP eller åben router. Gratis. Databricks og andre systemer kan kalde API'et udefra.
  - [ ] **API key autentificering** — simpel Bearer token header. Versionerede endpoints (`/v1/gold/heart_rate`). Rate limiting.
  - [ ] **Databricks custom connector** — Databricks notebook/job der kalder health API og skriver til Delta table. Demonstrerer health platform som first-class data source. PoC-værdi til Pandora.
  - [ ] **Custom Spark data source** — pakker HTTP-kaldet som en rigtig Spark connector: `spark.read.format("health_api").option("entity", "heart_rate").load()`. Enterprise-grade PoC.
- [ ] **Ugentlig health digest** — automatisk Markdown-rapport hver mandag via GitHub Actions cron. Forrige uge vs ugen før på tværs af alle silver-tabeller. Ingen ny infrastruktur.
- [ ] **Anomali-detektor** — flag usædvanlige målinger (meget høj puls, meget lav søvnscore) med simpel statistik (z-score eller percentil). Ren SQL/Python.
- [ ] **Master data annoterings-app** — lokal Streamlit-app til manuel annotation af kontekst wearables ikke kan fange: sygdom, rejse, stress, ny træningsrutine. Gemmes i `gold.daily_annotations` og joines på alle gold-views.

### Tier 2 — Høj værdi, mere arbejde

- [ ] **Lokal Streamlit-dashboard** — interaktivt health dashboard der kører på Mac Mini og læser direkte fra DuckDB eller health API. Flersidet layout: søvn / aktivitet / vitals / trends. Tilgængeligt fra telefon/iPad via lokalt netværk.
- [ ] **Korrelationsmotor** — automatisk find sammenhænge på tværs af metrics. "Dage med søvnscore > 80 giver X% lavere hvilepuls næste dag." Ren SQL, ingen ML.
- [ ] **Composite health score** — ét dagligt tal beregnet fra søvn + aktivitet + stress + readiness. Defineres som en gold-view med vægtede inputs.
- [ ] **Withings connector** — se dedikeret sektion. Withings: vægt, fedt%, muskelmasse, blodtryk. PoC for "plug in any source with one YAML".
- [ ] **Strava connector** — se dedikeret sektion. Løb, cykling, roning — distance, tempo, HR-zoner, elevation.
- [ ] **Min Sundhed connector** — se dedikeret sektion. Kliniske data fra sundhed.dk: blodprøver, diagnoser, medicin.
- [ ] **Familie-platform** — tilføj `user_id` dimension. Arkitekturen understøtter allerede source isolation — relativt lille refaktor at tracke flere brugeres data i samme platform.

### Tier 3 — Enterprise PoC materiale

- [ ] **Databricks AI/BI dashboard** — BI-værktøj direkte på gold-laget. Viser trends, scores og anomalier. Ingen ekstra infrastruktur når Databricks er konfigureret.
- [ ] **Databricks Genie Space** — natural language interface til gold-data. "Hvad var min bedste uge i januar?" Killer demo til Pandora. Kræver kun gold-tabeller + Databricks konfiguration.
- [ ] **Genie Everywhere — Microsoft Teams** — embed Genie Space i Teams via Copilot Studio. Ingen kode, få klik, OAuth per bruger. Relevant for dag-job: eliminerer ad-hoc "kan du trække..."-forespørgsler. Kræver Pro SQL warehouse (ikke Free Edition).
- [ ] **Claude API + SQL backend** — alternativ til Genie uden Databricks-afhængighed. FastAPI + DuckDB/Postgres backend med Claude API til natural language → SQL. Ejer hele stacken, ingen vendor lock-in. Kan sælges som produkt — Genie-integration gemmes til konsulent-engagementer.
- [ ] **Databricks PoC accelerator** — pak dette repo som en "30-minute Databricks PoC starter": medallion, CI/CD, dev/prd, metadata-driven. Brug internt hos Pandora som accelerator til nye dataprojekter.
- [ ] **Forudsigelsesmodel** — baseret på gårsdagens søvn + aktivitet, forudsig dagens readiness. Simpel lineær regression. Kræver minimum 6 måneders historisk data.

### Tier 4 — Passiv indkomst

- [ ] **Health data platform template** — sælg hele arkitekturen (medallion + connectors + dashboard) som Databricks-template til data engineers der vil tracke eget helbred. Udgangspunkt: dette repo poleret og generaliseret. Pris: $149-299.
- [ ] **Personal health API som SaaS** — hosted version af health API med Oura/Apple Health integration. Månedlig subscription for andre der vil have det samme setup uden selv at bygge det.
- [ ] **Anonymiseret benchmark** — "Din søvnscore er i top 30% for mænd 35-45." Kræver opt-in data fra andre brugere og privacy-arkitektur.

## Data Strategy

> ⚠️ **This section requires a dedicated interview session — do not build from this draft alone.**
> The items below are a rough draft created without proper input. Before any implementation,
> schedule a structured interview/workshop session where the owner (Claus) defines vision,
> priorities, and principles from scratch. Use the draft as a starting point for questions,
> not as a specification.

The overarching direction for the platform — why we build it, where it goes, and how decisions are made. Foundation for all governance, MDM, and quality work below.

- [ ] **Schedule data strategy interview session** — dedicated session where Claude interviews Claus on: platform vision, target audience, data domains, guiding principles, 3-horizon roadmap, and PoC narrative. Output: `docs/data_strategy.md`.
- [ ] **Write `docs/data_strategy.md`** *(draft only — rewrite after interview)* — platform vision, guiding principles, target audience (personal + PoC demo), data domains (vitals, sleep, activity, nutrition, clinical, genetic), 3-horizon roadmap (1: stable bronze/silver/gold; 2: quality gates + governance; 3: AI/Genie/multi-tenant)
- [ ] **Define data domains** — formally name and bound each domain: which sources belong, who owns it, what questions it answers. Drives tagging, lineage, and Genie Space organisation.
- [ ] **Define decision framework** — when do we use DuckDB vs Databricks? When do we use dbt vs raw SQL? When do we use DLT vs Jobs? Document the criteria so future decisions are consistent.
- [ ] **Align strategy with PoC narrative** — every architectural decision should have a "why this demonstrates enterprise readiness" sentence. Document in `docs/data_strategy.md` so it can be used directly in Pandora presentations.

## Data Governance, MDM & Best Practices

Plan and implement enterprise-grade data governance. High PoC value for the Pandora context.

### Data Governance

- [ ] **Data Owner** — dokumentér ejerskab per datakilde: `source_system`, ansvarlig person, opdateringsfrekvens, sensitivitetsniveau
- [ ] **Data Lineage** — spor data fra råkilde → bronze → silver → gold. Implementér via Unity Catalog Lineage (automatisk i Databricks) + manuelt Mermaid-diagram i `docs/data_lineage.md`
- [ ] **Data Retention policy** — definer hvor længe rådata (bronze parquet) og transformerede data (silver/gold Delta) gemmes. Implementér TTL-sletning eller arkivering
- [ ] **Access Control** — Unity Catalog GRANT-statements: hvem må læse bronze/silver/gold? Implementér `GRANT SELECT ON SCHEMA` per lag. Dokumentér i `docs/access_control.md`
- [ ] **Audit log** — aktivér Unity Catalog audit log i Databricks. Spor hvem der læser/skriver hvad og hvornår
- [ ] **GDPR compliance** — kortlæg personhenførbare data (PII) per tabel. Definer `right_to_erasure`-procedure: hvilke tabeller skal slettes hvis bruger beder om det?
- [ ] **Data Classification** — tagging af følsomhedsniveau per tabel/kolonne: `public`, `internal`, `confidential`, `sensitive` (f.eks. genetik, blodprøver)

### Master Data Management (MDM)

- [ ] **`user_id` dimension** — definer master `user_id` på tværs af alle kilder. Oura, Withings, Apple Health bruger alle forskellige bruger-identifikatorer — mapp dem til én canonical `user_id` i en `dim_user` tabel
- [ ] **`dim_user`** — master tabel: `user_id`, `user_name`, `date_of_birth`, `biological_sex`, `source_system_ids` (JSON map). Fundament for multi-tenant + familie-platform
- [ ] **`dim_source_system`** — master tabel over datakilder: `source_system`, `display_name`, `category` (wearable/nutrition/clinical/genetic), `ingest_frequency`, `owner`, `sla_hours`
- [ ] **Metric standardisering** — definer kanoniske metriknavne på tværs af kilder: `heart_rate_bpm` (ikke `hr`, ikke `bpm`, ikke `heartRate`). Dokumentér i `docs/metric_dictionary.md`
- [ ] **Enhedsstandardisering** — all distance i km, vægt i kg, temperatur i Celsius. Silver-laget konverterer ved ingest. Dokumentér konverteringsregler

### Data Tagging Plan

- [ ] **Unity Catalog Tags** — tag tabeller og kolonner med metadata i Databricks UC: `source_system`, `pii_level`, `freshness_sla`, `domain` (health/fitness/clinical/nutrition)
- [ ] **Kolonne-level PII tagging** — tag kolonner med `pii=true` for: navn, fødselsdato, GPS-koordinater, genetiske markører, diagnose-koder. Fundament for GDPR-maskering
- [ ] **Domæne-tagging** — gruppér tabeller i domæner: `vitals`, `sleep`, `activity`, `nutrition`, `clinical`, `genetic`. Bruges til governance-rapportering og Genie Space context
- [ ] **Databricks `manage_uc_tags` MCP tool** — implementér tagging via MCP: `mcp__databricks__manage_uc_tags` på alle eksisterende tabeller
- [ ] **Tag-konventioner** — dokumentér alle godkendte tags og værdier i `docs/tagging_conventions.md`. Undgå fri-tekst tags

### Data Quality Framework

- [ ] **`gold.data_quality_metrics`** — daglig DQ-tabel: `table_name`, `column_name`, `null_rate`, `distinct_count`, `min_value`, `max_value`, `anomaly_flag`, `checked_at`
- [ ] **DLT Expectations** — `@dlt.expect_or_drop` på silver-laget: heart_rate_bpm BETWEEN 30 AND 250, sleep_score BETWEEN 0 AND 100, weight_kg BETWEEN 20 AND 300
- [ ] **Schema enforcement** — aktivér Delta schema evolution policies: `mergeSchema=false` i prd, `mergeSchema=true` i dev. Breaking schema changes fejler explicit
- [ ] **Quarantine tables** — `bronze.quarantine_<source>` per kilde. Rækker der fejler DQ-tjek rutes hertil i stedet for at blive droppet. Audit-trail på hvad der gik galt
- [ ] **Freshness SLA** — se "Data SLA" sektion nedenfor — hænger direkte sammen med governance

### Best Practices — PoC Dokumentation

- [ ] **`docs/governance.md`** — samlet governance-dokument: ejerskab, klassifikation, retention, access control, GDPR-procedure
- [ ] **`docs/metric_dictionary.md`** — alle kanoniske metriknavne, enheder, beregningsregler, kildedefinitioner
- [ ] **`docs/data_lineage.md`** — Mermaid-diagram over data-flow fra kilde til gold
- [ ] **`docs/tagging_conventions.md`** — godkendte tags, værdier og anvendelse
- [ ] **`docs/access_control.md`** — GRANT-matrix: hvem har adgang til hvad

---

## Databricks Framework — Enterprise Scale-Up

Inspireret af [yasarkocyigit/daq-databricks-dab](https://github.com/yasarkocyigit/daq-databricks-dab) som PoC reference for enterprise-grade Databricks arkitektur.

- [ ] **DLT data quality expectations** — tilføj `@dlt.expect` / `@dlt.expect_or_drop` på silver entities. Giver automatisk row-level quality metrics i Databricks UI uden ekstra kode.
- [ ] **Quarantine tables** — rut fejlede rækker til `bronze.quarantine_<source>` i stedet for at droppe dem. Kræver én ekstra `WHEN NOT MATCHED BY SOURCE` arm i MERGE eller `expect_or_quarantine` i DLT.
- [ ] **DLT pipeline monitoring job** — separat DAB job der kører efter silver/gold og validerer: antal rækker, freshness (`max(_ingested_at) < NOW() - INTERVAL 25 HOURS`), og null-rate på nøglekolonner. Sender alert hvis threshold overskrides.
- [ ] **Gold `depends_on` ordering** — tilføj eksplicit `depends_on: [silver_pipeline]` i gold DAB job config. Sikrer korrekt rækkefølge og undgår stale gold views ved fejl i silver.

### Tier 5 — Vilde ideer. Ingen grænser.

- [ ] **Digital twin** — en simuleringsmodel af dig selv. Feed ind: "hvad sker der med min readiness hvis jeg sover 6 timer 3 nætter i træk + træner tungt?" Modellen simulerer svaret baseret på dine egne historiske mønstre. Ingen andre har det.

- [ ] **Biologisk aldersberegner** — kombinér HRV, hvilepuls, søvnkvalitet, VO2 max, BMI og aktivitetsniveau til ét tal: din biologiske alder. "Du er 38 år, men din krop er 31." Opdateres dagligt. Baseret på peer-reviewed biomarker-forskning.

- [ ] **AI health coach med fuld kontekst** — LLM der har adgang til *alle* dine health data + annotationer. Svarer på: "Hvorfor sov jeg dårligt i januar?" og "Hvad er den største forskel på mine bedste og dårligste uger?" Ikke generisk rådgivning — personaliseret til dit eget datasæt.

- [ ] **Sygdomsdetektion før symptomer** — HRV, hudtemperatur og SpO2 falder typisk 1-2 dage før du mærker du er syg. Træn en model på dine egne data der flagger: "noget er på vej." Oura gør det allerede delvist — din version er transparent og forklarbar.

- [ ] **Miljøkorrelationsmotor** — automatisk kryds-reference med vejr-API, luftkvalitets-API og pollendata. "Din søvn er 18% dårligere på dage med høj pollentælling." Ren dataingeniør-opgave, nul ML.

- [ ] **Health-aware kalender** — integrer med Google Calendar. Analyser dine energi- og fokus-mønstre (hvornår er din HRV højest? hvornår sover du bedst?) og foreslå automatisk: "Sæt dine vigtigste møder tirsdag-torsdag 9-11. Undgå deep work mandag morgen."

- [ ] **Real-time health streaming** — Kafka + Spark Structured Streaming pipeline. Oura sender events i næsten-realtid. I stedet for batch-ingest én gang i døgnet: kontinuerlig stream ind i Delta. Fundament for live dashboard og instant anomali-alerts.

- [ ] **Corporate wellness platform** — samme arkitektur, `user_id` dimension, multi-tenant deployment. Sælg til virksomheder som et anonymiseret wellness-dashboard: "Jeres teams gennemsnitlige søvnscore er faldet 12% siden Q3." GDPR-compliant by design fordi alt er aggregeret og opt-in.

- [ ] **Federated health intelligence** — træn ML-modeller på tværs af brugere *uden* at dele rådata. Kun model-gradienter synkroniseres. Federated learning betyder at din data aldrig forlader din enhed — men du drager fordel af 1000 brugeres mønstre. Banebrydt inden for sundhedsdata.

- [ ] **Genomik-integration** — connect 23andMe eller AncestryDNA til platformen. Kryds-referencér genetiske markører med dine wearable-data. "Du har APOE-variant der øger søvnbehov — og dine data bekræfter det: du performer 22% bedre med 8+ timers søvn."

- [ ] **Sundhedsdata-pas** — kryptografisk verificerbart credential (W3C Verifiable Credential standard). Du kan bevise over for en læge, forsikringsselskab eller arbejdsgiver: "min gennemsnitlige hvilepuls er 52 bpm, verificeret af mit eget datasystem" — uden at udlevere rådata. Selvbestemmelse over egne data.

- [ ] **Stemmesundhedsanalyse** — optag 30 sekunders tale hver morgen. Analyser akustiske features (pitch-variation, tale-tempo, pausemønstre) der korrelerer med stress, udmattelse og depression. Ingen invasiv sensor — bare din telefons mikrofon.

## Tools at installere

- [ ] Google Antigravity — `antigravity.google/download` → Download for Apple Silicon
- [ ] ~~GSD (Get Shit Done)~~ — **PAUSE** (2026-02-26). Crypto-token tilknyttet, anbefaler `--dangerously-skip-permissions`, global npm-install. Revurdér maj 2026 — er den stadig aktiv og ren, kan vi kigge på det.
- [ ] Figma — opret gratis Starter profil
- [ ] Apify — aktivér som Claude Connector
- [ ] Wispr Flow — evaluér til voice-to-text

## Mac Mini setup

- [ ] Disable sleep — aldrig slukke
- [ ] Enable auto-restart after power failure
- [ ] SSH adgang
- [ ] Claude Desktop kører altid (krav for Cowork scheduled tasks)

## Claude Cowork — Scheduled Jobs

Peg Cowork mod `~/builder-automation/`

| Tid | Job |
|-----|-----|
| 06:00 | Apify scraper content fra Reddit, LinkedIn, YouTube, Twitter/X → Notion |
| 07:00 | Claude genererer LinkedIn drafts fra scraped content → Notion |
| 07:00 mandag | Konkurrentresearch: scrape Gumroad, Etsy, Lemon Squeezy → Notion |
| 08:00 | Post approved drafts til LinkedIn, Reddit, Twitter/X, GitHub |
| 22:00 | Saml metrics: engagement, followers, sales → Notion dashboard |
| On-demand | Oversæt produkter til spansk, tysk, fransk, kinesisk |

Opsæt via `/schedule` i Cowork.

## GitHub repos og profiler til inspiration

- [ ] **[databrickslabs](https://github.com/databrickslabs)** — Databricks Labs org: acceleratorer, tools og PoC-mønstre. Gennemgå repos der er relevante for medallion, CI/CD og data quality.
- [ ] **[databricks-solutions/ai-dev-kit](https://github.com/databricks-solutions/ai-dev-kit)** — Officielt Databricks AI dev kit. Evaluer om patterns herfra kan løfte PoC-kvaliteten.
- [ ] **[great-expectations](https://github.com/great-expectations)** — Data quality og validering. Naturligt næste skridt efter dbt tests — evaluer som enterprise-grade DQ-lag oven på silver.
- [ ] **[mauran/API-Danmark](https://github.com/mauran/API-Danmark)** — Samling af danske offentlige APIs. Kan bruges som datakilde eller inspiration til connector-mønster i platformen.

## Repo arbejde

- [x] Gennemgang af hele repo for optimering potentiale — se Optimering sektion
- [ ] Gennemgå best practice og se .md filen — brug den
- [ ] Start plan mode og arbejd fra todo
- [ ] Kør GSD `/gsd:discuss` på repo

## Optimering — Quick Wins (< 30 min)

Fundinger fra repo-gennemgang 2026-02-26.

- [ ] **P0 — Struktureret logging** — erstat alle `print()` med `logging`-module i `ingestion_engine.py`. Giver sporbare logs på Databricks og lokal debugning.
- [ ] **P0 — Path-validering** — tilføj `if not input_path.exists(): sys.exit(...)` i `csv_to_parquet.py` før `pd.read_csv()`. Fejler med klar besked i stedet for cryptic pandas-fejl.
- [ ] **P1 — Konsistent path-løsning** — `ingestion_engine.py` bruger hardcoded relativ path; `run_merge.py` bruger `Path(__file__).resolve().parents[4]`. Sammenskriv til fælles `get_config_path()` util.
- [ ] **P1 — YAML schema-validering** — tilføj `jsonschema`-validering af `sources_config.yaml` og `environment_config.yaml` ved load. Runtime-fejl bliver parse-fejl med præcis besked.

## Optimering — Større Opgaver (> 30 min)

- [ ] **P2 — dbt schema-tests** — tilføj `tests:` blokke i silver `schema.yml` med not-null, unique og range-validering per silver-entitet. Kræver ~5-10 tests per tabel × 17 tabeller.
- [ ] **P2 — Persistent state-tracking** — opret `bronze._state_tracking`-tabel med `last_fetched_date` per kilde/endpoint. Eliminerer hardcoded `DEFAULT_LOOKBACK_DAYS=90` og gør incremental load auditable.
- [ ] **P2 — Merge-scripts refactor** — 17 merge-scripts har sandsynligvis duplikeret MERGE INTO-logik. Refactor til single parameterized template i stil med `silver_runner.py`.
- [ ] **P3 — End-to-end integration tests** — pytest-suite der kører fuld medallion-pipeline: bronze ingestion → silver merge → row count validation + schema evolution test.
- [ ] **P4 — Delta Live Tables migration** — refactor Jobs til DLT med `@dlt.expect` for automatic retry, schema-versioning og data quality SLAs. Enterprise/PoC-milestone.

## Claude Code subagents

- ~~Explorer — scan codebase for snippets og afhængigheder~~ — built-in, dokumenteret i CLAUDE.md
- ~~Researcher — hent ekstern docs (Synapse, Databricks)~~ — built-in, dokumenteret i CLAUDE.md
- ~~Historian — context/hukommelse på tværs af sessions~~ — built-in via memory-system

## Figma

- [ ] Opret profil
- [ ] Test Claude Code → Figma MCP flow
- [ ] Brug til design-samarbejde med kone på HR-produkter

## Learning & Certification

### Free badges (quick wins — tag dem nu)

- [ ] **Lakehouse Fundamentals** — 4 korte videoer + quiz → LinkedIn badge. [databricks.com/learn/training](https://www.databricks.com/learn/training/home)
- [ ] **Generative AI Fundamentals** — 4 korte videoer → LinkedIn badge
- [ ] **AI Agent Fundamentals** — LinkedIn badge
- [ ] **AWS Platform Architect accreditation** — gratis assessment → badge
- [ ] **Platform Administrator accreditation** — gratis learning pathway + assessment

### Betalte certificeringer (~$200 pr. stk)

- [ ] **Data Engineer Associate** — direkte relevant, første prioritet. Forbered med free training + Databricks Academy.
- [ ] **Data Engineer Professional** — efter Associate er bestået.

### Events

- [ ] **Data + AI Summit 2026** — 15-18 juni, San Francisco. Sæt i kalenderen. Overvej om det er værd at deltage (community edition tickets / remote stream).

---

## Content idéer

- "How I use Claude Code subagents as a data engineer"
- "My Mac Mini runs 24/7 with Claude Cowork"
- Claude Code + GSD workflow dokumentation
- Build in public om hele setuptet
- "How I set up Databricks Genie in Teams in 30 minutes" (LinkedIn)
- "3 OAuth patterns for embedding Genie in your apps" (technical authority)
- "Why your data team should stop answering ad-hoc questions" (problem-aware)
- Screen recording: Genie + Copilot Studio → Teams setup (YouTube/TikTok)

## Droppet

- ~~OpenClaw~~ — erstattet af Claude Cowork + Apify connector
- ~~Ollama lokalt~~ — kvaliteten er for lav til agentic workflows, brug Gemini 3 Pro gratis i Antigravity

## Quality & Testing

- [ ] **dbt tests** — add `schema.yml` with not-null, unique, accepted-values tests per silver entity
- [ ] **Freshness checks** — validate last ingested timestamp per source (bronze → silver lag)
- [ ] **Row count reconciliation** — bronze vs silver row counts after each merge run

## Cleanup

- [ ] **`health_environment/deployment/databricks/`** — catalog/schema DDL scripts (`create_catalog__health_dw.sql`, `create_schemas__health_dw.sql`) may be superseded by `init.py` — consider archiving

## Nye datakilder — iCloud sundhedsdata

Kilde: `/Users/Shared/sundhedsdata`

- [ ] **Blodprøve (PDF)** — parse PDF-rapport til strukturerede rækker → `silver.blood_test`. Felter: markør, værdi, enhed, referenceserie, dato. Connector: Python PDF-parser (pdfplumber).
- [ ] **23andMe genetisk data (CSV + JSON + PDF)** — importer raw genotype CSV + health report JSON → `silver.genetic_ancestry`, `silver.genetic_health_risk`. Hold kilde-filnavne som `source_file` metadata.
- [ ] **Mikrobiome (PDF)** — parse mikrobiome-rapport → `silver.microbiome_snapshot`. Felter: bakterie-gruppe, procent, reference-range, dato.

Alle tre følger standard medallion-mønster: raw fil → bronze parquet → silver merge via YAML-config.

## Silver — Dato og Tidsdimensioner

Port fra `archive/legacy_databricks_pipeline/silver/` — bevar alle navngivninger.

- [x] **`dim_date`** — portet til `transformation_logic/databricks/silver/dim_date.py`. Kører MERGE INTO `health_dw.silver.dim_date`. ~~`archive/.../notebook/date.py` slettet.~~
- [x] **`dim_time`** — portet til `transformation_logic/databricks/silver/dim_time.py`. Kører MERGE INTO `health_dw.silver.dim_time`. ~~`archive/.../notebook/time.py` slettet.~~
- [ ] **Lokalisering `dim_date`** — `month_name` og `day_name` er aktuelt på engelsk (Spark default). Tilføj dansk lokale: `month_name` → januar/februar/.../december, `day_name` → mandag/tirsdag/.../søndag. Implementering: enten `F.when()` lookup-kæde eller Spark session locale `spark.conf.set("spark.sql.session.timeZone", ...)` + custom mapping dict.
- [ ] **Lokalisering `dim_time`** — `ampm_code` er AM/PM (engelsk). Overvej dansk dagsperiode-kolonne: `dagsperiode` → nat (00-06), morgen (06-09), formiddag (09-12), middag (12-14), eftermiddag (14-17), aften (17-22), sen aften (22-00).
- [ ] **YAML-config** — tilføj `dim_date` og `dim_time` som entries i `sources_config.yaml` (type: `generated`, ikke parquet-source). Følg metadata-driven mønster.
- [ ] **Gold joins** — når dim_date og dim_time er i silver, tilføj dato/tid join til relevante gold-views (heart_rate, sleep, activity).

### Legacy SQL — portet og slettet fra archive

Alle filer er portet som `.py` Databricks notebooks og archive-kopierne er slettet.

| Fil | Portet til (SQL) | Archive slettet |
|-----|-----------------|-----------------|
| `blood_oxygen` | `silver/sql/blood_oxygen.sql` | [x] |
| `blood_pressure` | `silver/sql/blood_pressure.sql` | [x] |
| `daily_activity` | `silver/sql/daily_activity.sql` | [x] |
| `daily_annotations` | `silver/sql/daily_annotations.sql` | [x] |
| `daily_meal` | `silver/sql/daily_meal.sql` | [x] |
| `daily_readiness` | `silver/sql/daily_readiness.sql` | [x] |
| `daily_sleep` | `silver/sql/daily_sleep.sql` | [x] |
| `heart_rate` | `silver/sql/oura_heart_rate.sql` (renamed — `heart_rate.sql` er YAML-template) | [x] |
| `weight` | `silver/sql/weight.sql` | [x] |

**Gold views** portet:
- [x] `vw_daily_annotations.sql` → `gold/sql/vw_daily_annotations.sql`
- [x] `vw_heart_rate_avg_per_day.sql` → `gold/sql/vw_heart_rate_avg_per_day.sql`

### Silver notebooks — oprydning og migrering til SQL + dbt

De portede notebooks er aktuelt `.py` filer med `# MAGIC %sql` blokke (Databricks notebook-format). Dette er en mellemløsning — målet er rene SQL-filer:

- [x] **Ryd op i Python-wrappere** — konverteret alle 9 silver + 2 gold `.py` magic-notebook filer til rene `.sql` filer i `silver/sql/` og `gold/sql/`. `.py` wrappers slettet (PR #19).
- [ ] **dbt-version** — skriv alle 9 silver-tabeller som dbt models (`models/silver/*.sql`) med `{{ config(materialized='incremental', unique_key=...) }}`. Dette er enterprise-mønsteret og understøtter både DuckDB (lokalt) og Databricks (cloud). dbt håndterer MERGE/INSERT OVERWRITE automatisk via incremental strategy.
- [ ] **Opdater kildereferencer** — alle notebooks peger på `workspace.default.*` (legacy Databricks workspace). Opdater til `health_dw.bronze.stg_*` når bronze-laget er klart.
- [ ] **Vælg langsigtet strategi** — beslut om silver-laget skal køres som (a) rene Databricks SQL notebooks, (b) dbt-core med dbt-databricks adapter, eller (c) begge parallelt (dbt lokalt på DuckDB + Databricks notebooks i cloud). Dokumentér valget i `docs/architecture.md`.

## Scheduled Databricks Jobs — Fuld Automatisk Data Load

Mål: kilde → rapport uden manuel indgriben. Alle jobs kører på Databricks med DAB job configs.

| Job | Frekvens | Kilde | Output |
|-----|----------|-------|--------|
| Oura ingest | Daglig 03:00 UTC | Oura REST API | `bronze.stg_oura_*` → `silver.*` |
| Withings ingest | Daglig 03:30 UTC | Withings REST API | `bronze.stg_withings_*` → `silver.*` |
| Strava ingest | Daglig 03:45 UTC | Strava REST API | `bronze.stg_strava_*` → `silver.*` |
| Apple Health ingest | Ugentlig man 04:00 UTC | Zip → XML → parquet | `bronze.stg_apple_*` → `silver.*` |
| Lifesum ingest | Daglig 04:00 UTC | Lifesum scraper/agent | `bronze.stg_lifesum_*` → `silver.*` |
| Min Sundhed ingest | Ugentlig man 04:30 UTC | FHIR API / scraper | `bronze.stg_minsundhed_*` → `silver.*` |
| Gold refresh | Daglig 05:00 UTC | Silver → Gold | Alle gold views/tabeller |
| SLA monitor | Daglig 06:00 UTC | Gold lag | Alert hvis SLA brydes |

- [ ] **Oura daily job** — DAB job: kald Oura REST API → parquet → bronze → silver. Brug eksisterende `ingestion_engine.py` + `silver_runner.py`.
- [ ] **Withings daily job** — DAB job: kald Withings REST API → parquet → bronze → silver.
- [ ] **Strava daily job** — DAB job: kald Strava REST API → parquet → bronze → silver.
- [ ] **Apple Health weekly job** — DAB job: trigger zip-flow (se nedenfor) → XML → parquet → bronze → silver. Kør mandag.
- [ ] **Lifesum daily job** — DAB job: kald Lifesum scraper/agent (se nedenfor) → parquet → bronze → silver.
- [ ] **Min Sundhed weekly job** — DAB job: kald FHIR API / scraper → parquet → bronze → silver. Kør mandag.
- [ ] **Gold refresh job** — DAB job: kør alle gold SQL transforms efter silver. `depends_on: [silver_pipeline]`.
- [ ] **SLA monitor job** — DAB job: valider freshness + row count per kilde. Alert via webhook/e-mail hvis SLA brydes.
- [ ] **DAB job configs** — definer alle ovenstående i `databricks.yml`. Dev-workspace kører samme jobs med `--dev`-flag.

## Apple Health Auto-Ingest — Zip → XML → Parquet

Automatisk flow fra iCloud export til bronze parquet uden manuel indgriben.

- [ ] **Zip-detektion og -flytning** — Python-script der overvåger (`/Users/Shared/sundhedsdata/`) for ny `export.zip`. Kopierer til Mac Mini lokal arbejdsmappe (`/Users/Shared/data_lake/apple_health/raw/`).
- [ ] **Automatisk unzip** — script unzipper til `~/health_data/apple_health/unzipped/YYYY-MM-DD/`. Bevarer dato-versioning så gamle exports ikke overskrives.
- [ ] **XML → Parquet** — kalder eksisterende `xml_to_parquet.py` på den udpakkede `export.xml`. Output: parquet-filer per type i `~/health_data/apple_health/parquet/`.
- [ ] **End-to-end wrapper** — enkelt Python-orchestrator der kæder: zip-detektion → flytning → unzip → XML parsing → bronze ingestion. Trigges af Databricks weekly job via SSH eller lokalt launchd.
- [ ] **Launchd plist (Mac Mini)** — lokalt `launchd`-job der kigger efter ny zip ugentligt og kører wrapper automatisk. Backup-trigger uafhængig af Databricks.

## Lifesum Connector — Daglig Mad Data

Automatisk download af ugerapport fra Lifesum. Kør dagligt — altid frisk mad-data for de seneste 7 dage.

- [ ] **Lifesum login-agent** — Python-script der logger ind på Lifesum med credentials fra `.env` (aldrig hardcoded). Brug Playwright til headless browser-login → navigér til export-side → download CSV/JSON for de seneste 7 dage.
- [ ] **Fallback: Lifesum API/GDPR export** — undersøg om Lifesum tilbyder officiel export-API eller GDPR data export endpoint der kan automatiseres uden headless browser.
- [ ] **Bronze ingest** — gem downloaded rapport som parquet → `bronze.stg_lifesum_food_log`. Felter: dato, måltid, fødevare, kalorier, protein, kulhydrat, fedt, fiber.
- [ ] **Silver transform** — `silver.lifesum_food_log` med standardiserede kolonner og `_ingested_at`. YAML-config + `silver_runner.py`.
- [ ] **Credentials** — Lifesum email + password i `.env` (lokalt) og Databricks secrets (prd). Dokumentér i `.env.example`.

## Web App — Mobile & Browser Dashboard

Tilgængeligt fra iPhone og desktop browser. Viser gold-lag data live.

- [ ] **Framework** — Streamlit MVP (hurtigst, kører på Mac Mini) → eksponeret via Cloudflare Tunnel. Næste iteration: React/Next.js for bedre mobil UX.
- [ ] **Streamlit MVP** — læser fra DuckDB (dev) eller Health API (prd). Kør på Mac Mini port 8501.
- [ ] **Mobil-adgang** — eksponér via Cloudflare Tunnel (se `Personal health API → Cloudflare Tunnel`). HTTPS fra iPhone Safari uden åben router.
- [ ] **Autentificering** — Cloudflare Access (gratis) som login-gate foran appen.
- [ ] **Dashboard sider**:
  - [ ] **Overblik** — composite health score, dagens metrics, SLA-status (grøn/rød per kilde)
  - [ ] **Søvn** — søvnscore, søvnfaser, HRV-trend
  - [ ] **Aktivitet** — skridt, aktive kalorier, træning
  - [ ] **Ernæring** — kalorier, makros (Lifesum), trends
  - [ ] **Vitals** — hvilepuls, blodilt, vægt (Withings)
  - [ ] **Anomalier** — flag usædvanlige målinger
- [ ] **Responsivt layout** — brug `st.columns()` der fungerer på mobil. Test på iPhone Safari.

## Data SLA — Freshness & Quality Garantier

SLA defineret per datakilde. Monitoreringsjob kører dagligt kl. 06:00 UTC og skriver til `gold.data_sla_status`.

| Kilde | Freshness SLA | Min. rækker | Alert |
|-------|--------------|-------------|-------|
| Oura | < 25 timer | > 0 / dag | E-mail + webhook |
| Withings | < 25 timer | > 0 / dag | E-mail + webhook |
| Strava | < 25 timer | > 0 / dag | E-mail + webhook |
| Apple Health | < 8 dage | > 100 / uge | E-mail |
| Lifesum | < 25 timer | > 0 / dag | E-mail + webhook |
| Min Sundhed | < 8 dage | > 0 / uge | E-mail |

- [ ] **`gold.data_sla_status`** — tabel med kolonner: `source_system`, `last_ingested_at`, `hours_since_last_ingest`, `sla_hours`, `sla_met`, `actual_rows`, `min_rows_sla`, `rows_sla_met`, `checked_at`.
- [ ] **SLA monitor notebook** — populerer `gold.data_sla_status` og sender alert hvis `sla_met = false`. Brug Databricks notification destinations (e-mail / Slack webhook).
- [ ] **SLA widget i web app** — øverste sektion i dashboard: grøn/rød status per kilde med tidsstempel for sidst opdateret.
- [ ] **Quarantine tracking** — inkludér antal rækker i `bronze.quarantine_*` per kilde i SLA-rapporten. Høj quarantine-rate = datakvalitetsproblem.
- [ ] **Alert kanal** — konfigurér Databricks notification destination + Slack/e-mail webhook i `.env`.

## Fuld Data Model — Alle Kilder

Dokumentér det komplette skema for alle bronze/silver/gold tabeller på tværs af alle kilder. Fundament for connector-udvikling og gold-views.

- [ ] **Kildeoversigt** — kortlæg alle sources og deres raw attributter. Skabelon per kilde: `source_system`, `endpoints`, `raw felter`, `update-frekvens`, `nøgle til deduplication`.
- [ ] **Bronze skema** — dokumentér alle `stg_*` tabeller: kolonner, typer, partitionering, `_ingested_at`. En tabel per endpoint/filtype.
- [ ] **Silver skema** — dokumentér alle standardiserede silver-tabeller: rensede typer, renamed kolonner, `sk_`-nøgler, `_source_system`, `_valid_from`. Inkludér `dim_date` og `dim_time` relationer.
- [ ] **Gold skema** — dokumentér alle gold-views og fact/dim tabeller. Hvilke silver-tabeller joines? Hvilke metrics beregnes?
- [ ] **ERD (Entity Relationship Diagram)** — tegn relationer: `dim_date` ← `fact_*`, `user_id` på tværs. Gem som `docs/data_model.md` + evt. Mermaid-diagram.
- [ ] **Kildetabel**:

| Kilde | Frekvens | Bronze tabeller | Silver tabeller |
|-------|----------|-----------------|-----------------|
| Oura | Daglig | `stg_oura_sleep`, `stg_oura_readiness`, `stg_oura_activity`, `stg_oura_heart_rate`, `stg_oura_spo2`, `stg_oura_stress`, `stg_oura_tags` | `oura_sleep`, `oura_readiness`, `oura_activity`, `oura_heart_rate`, `oura_spo2` |
| Withings | Daglig | `stg_withings_measurements`, `stg_withings_sleep`, `stg_withings_activity` | `withings_body_composition`, `withings_blood_pressure`, `withings_sleep`, `withings_activity` |
| Apple Health | Ugentlig | `stg_apple_*` (per XML type) | `apple_heart_rate`, `apple_steps`, `apple_workouts`, `apple_sleep`, `apple_hrv` |
| Lifesum | Daglig | `stg_lifesum_food_log` | `lifesum_food_log` |
| Strava | Daglig | `stg_strava_activities`, `stg_strava_streams` | `strava_activities`, `strava_hr_zones` |
| Min Sundhed | Ugentlig | `stg_minsundhed_observations`, `stg_minsundhed_conditions`, `stg_minsundhed_medications` | `minsundhed_lab_results`, `minsundhed_diagnoses`, `minsundhed_medications` |
| Blodprøve PDF | Manuel | `stg_blood_test_pdf` | `blood_test` |
| 23andMe | Engangs | `stg_23andme_genotype` | `genetic_ancestry`, `genetic_health_risk` |
| Mikrobiome PDF | Manuel | `stg_microbiome_pdf` | `microbiome_snapshot` |
| Generated | — | — | `dim_date`, `dim_time` |

## Withings API Connector

Withings Health Mate API (OAuth2). Data: kropsammensætning, blodtryk, søvn, aktivitet.

- [ ] **OAuth2 flow** — registrér app på developer.withings.com. Client ID/secret i `.env`. Authorization code flow med refresh token. Gem tokens i `~/.config/health_reporting/withings_tokens.json`.
- [ ] **Endpoints**:
  - `/measure?action=getmeas` — vægt, BMI, fedt%, muskelmasse, knoglemasse, hydration, blodtryk, puls
  - `/v2/sleep?action=get` — søvnfaser (light, deep, REM), søvnscore, snorken, apnø-flag
  - `/v2/activity?action=getactivity` — skridt, kalorier, distance, aktive minutter, elevation
- [ ] **Bronze ingest** — `stg_withings_measurements`, `stg_withings_sleep`, `stg_withings_activity`. Partitionering: `year/month/day`. Felter fra API bevares råt + `_ingested_at`.
- [ ] **Silver transforms**:
  - `silver.withings_body_composition` — vægt, fedt%, muskelmasse, BMI, hydration
  - `silver.withings_blood_pressure` — systolisk, diastolisk, puls, dato/tid
  - `silver.withings_sleep` — søvnstart, søvnslut, faser, score
  - `silver.withings_activity` — dato, skridt, kalorier, distance, aktive_min
- [ ] **YAML-config** — tilføj alle Withings endpoints i `sources_config.yaml`. Følg Oura-mønster.
- [ ] **Credentials** — `WITHINGS_CLIENT_ID`, `WITHINGS_CLIENT_SECRET`, `WITHINGS_ACCESS_TOKEN`, `WITHINGS_REFRESH_TOKEN` i `.env.example`.

## Strava API Connector

Strava REST API (OAuth2). Data: aktiviteter (løb, cykling, roning m.m.) med detaljerede metrics.

- [ ] **OAuth2 flow** — registrér app på strava.com/settings/api. Scope: `activity:read_all`. Client ID/secret i `.env`. Refresh token-flow. Gem tokens i `~/.config/health_reporting/strava_tokens.json`.
- [ ] **Endpoints**:
  - `GET /athlete/activities` — liste af aktiviteter: type, dato, distance, varighed, elevation, avg/max HR, kalorier
  - `GET /activities/{id}/streams` — detaljerede tidsserier: HR, pace, power, cadence, altitude, lat/lon per sekund
  - `GET /athlete/zones` — HR-zoner defineret af atleten
- [ ] **Rate limits** — 100 req/15 min, 1000 req/dag. Implementér exponential backoff + request queue.
- [ ] **Bronze ingest** — `stg_strava_activities` (én række per aktivitet), `stg_strava_streams` (tidsserier). Partitionering: `year/month/day`.
- [ ] **Silver transforms**:
  - `silver.strava_activities` — aktivitetstype, dato, distance_km, varighed_sek, elevation_m, avg_hr, max_hr, kalorier, avg_pace
  - `silver.strava_hr_zones` — distribution af tid i HR-zone 1-5 per aktivitet
- [ ] **YAML-config** — tilføj Strava endpoints i `sources_config.yaml`.
- [ ] **Credentials** — `STRAVA_CLIENT_ID`, `STRAVA_CLIENT_SECRET`, `STRAVA_ACCESS_TOKEN`, `STRAVA_REFRESH_TOKEN` i `.env.example`.

## Min Sundhed Connector — sundhed.dk

Kliniske data fra det danske nationale sundhedsregister via sundhed.dk. Login kræver MitID.

- [ ] **Adgangsmetode — undersøg**:
  - **Option A: Sundhedsdatastyrelsen FHIR R4 API** — officiel API med NemID/MitID OAuth. Endpoints: `/Patient`, `/Observation` (lab), `/Condition` (diagnoser), `/MedicationRequest`. Kræver ansøgning om adgang.
  - **Option B: Playwright scraper** — headless MitID-login på sundhed.dk → navigér til "Mine data" → download PDF/CSV eksport. Kompleks pga. MitID-flow.
  - **Option C: FHIR eksport** — sundhed.dk tilbyder manuel FHIR-eksport (JSON). Automatisér download via Playwright post-login.
  - Anbefalet: start med Option C (manuel), bygge mod Option A (automatisk).
- [ ] **Bronze ingest** — `stg_minsundhed_observations` (laboratorieresultater), `stg_minsundhed_conditions` (diagnoser), `stg_minsundhed_medications` (medicin), `stg_minsundhed_vaccinations`.
- [ ] **Silver transforms**:
  - `silver.minsundhed_lab_results` — markør (LOINC/lokalkode), værdi, enhed, referenceinterval, dato, laboratorium
  - `silver.minsundhed_diagnoses` — ICD10-kode, beskrivelse, dato, behandlingssted
  - `silver.minsundhed_medications` — ATC-kode, præparat, dosis, startdato, status
- [ ] **Join med blodprøve-PDF** — `silver.blood_test` (PDF-parser) + `silver.minsundhed_lab_results` kan joines på markør + dato for dobbeltvalidering.
- [ ] **Credentials** — MitID-credentials aldrig i kode. Gem krypteret i macOS Keychain. Tilgås via `keyring`-bibliotek.
