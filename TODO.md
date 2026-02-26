# TODO

## Configuration

- [ ] **`databricks.yml` workspace URLs** — replace placeholder hosts with real Databricks workspace URLs for `dev` and `prd` targets in `health_unified_platform/health_environment/deployment/databricks/databricks.yml`
- [ ] **`.env.example`** — document required environment variables (Oura OAuth client ID/secret, Databricks host, token, catalog name, `HEALTH_ENV`)
- [ ] **Re-enable branch protection status check** — add `Validate bundle` as required check on `main` once `DATABRICKS_HOST_DEV` and `DATABRICKS_TOKEN_DEV` GitHub secrets are configured. Disabled 2026-02-26 pending Databricks setup.

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
- [ ] **Withings / Garmin connector** — flere datakilder ind i samme medallion-arkitektur. Withings: vægt, blodtryk. Garmin: avancerede træningsmetrics. PoC for "plug in any source with one YAML".
- [ ] **Familie-platform** — tilføj `user_id` dimension. Arkitekturen understøtter allerede source isolation — relativt lille refaktor at tracke flere brugeres data i samme platform.

### Tier 3 — Enterprise PoC materiale

- [ ] **Databricks AI/BI dashboard** — BI-værktøj direkte på gold-laget. Viser trends, scores og anomalier. Ingen ekstra infrastruktur når Databricks er konfigureret.
- [ ] **Databricks Genie Space** — natural language interface til gold-data. "Hvad var min bedste uge i januar?" Killer demo til Pandora. Kræver kun gold-tabeller + Databricks konfiguration.
- [ ] **Databricks PoC accelerator** — pak dette repo som en "30-minute Databricks PoC starter": medallion, CI/CD, dev/prd, metadata-driven. Brug internt hos Pandora som accelerator til nye dataprojekter.
- [ ] **Forudsigelsesmodel** — baseret på gårsdagens søvn + aktivitet, forudsig dagens readiness. Simpel lineær regression. Kræver minimum 6 måneders historisk data.

### Tier 4 — Passiv indkomst

- [ ] **Health data platform template** — sælg hele arkitekturen (medallion + connectors + dashboard) som Databricks-template til data engineers der vil tracke eget helbred. Udgangspunkt: dette repo poleret og generaliseret. Pris: $149-299.
- [ ] **Personal health API som SaaS** — hosted version af health API med Oura/Apple Health integration. Månedlig subscription for andre der vil have det samme setup uden selv at bygge det.
- [ ] **Anonymiseret benchmark** — "Din søvnscore er i top 30% for mænd 35-45." Kræver opt-in data fra andre brugere og privacy-arkitektur.

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

## Content idéer

- "How I use Claude Code subagents as a data engineer"
- "My Mac Mini runs 24/7 with Claude Cowork"
- Claude Code + GSD workflow dokumentation
- Build in public om hele setuptet

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

- [ ] **`dim_date`** — port `archive/legacy_databricks_pipeline/silver/notebook/date.py` + `table/date.sql` til ny silver-lag. Output: `silver.dim_date` med dag, uge, måned, kvartal, år, ugedagsnavn, helligdag-flag.
- [ ] **`dim_time`** — port `archive/legacy_databricks_pipeline/silver/notebook/time.py` + `table/time.sql` til ny silver-lag. Output: `silver.dim_time` med time, minut, sekund, AM/PM, dagsperiode.
- [ ] **YAML-config** — tilføj `dim_date` og `dim_time` som entries i `sources_config.yaml` (type: `generated`, ikke parquet-source). Følg metadata-driven mønster.
- [ ] **Gold joins** — når dim_date og dim_time er i silver, tilføj dato/tid join til relevante gold-views (heart_rate, sleep, activity).

## Scheduled Databricks Jobs — Fuld Automatisk Data Load

Mål: kilde → rapport uden manuel indgriben. Alle jobs kører på Databricks med DAB job configs.

| Job | Frekvens | Kilde | Output |
|-----|----------|-------|--------|
| Oura ingest | Daglig 03:00 UTC | Oura REST API | `bronze.stg_oura_*` → `silver.*` |
| Withings ingest | Daglig 03:30 UTC | Withings REST API | `bronze.stg_withings_*` → `silver.*` |
| Apple Health ingest | Ugentlig man 04:00 UTC | iCloud zip → XML → parquet | `bronze.stg_apple_*` → `silver.*` |
| Lifesum ingest | Daglig 04:00 UTC | Lifesum scraper/agent | `bronze.stg_lifesum_*` → `silver.*` |
| Gold refresh | Daglig 05:00 UTC | Silver → Gold | Alle gold views/tabeller |
| SLA monitor | Daglig 06:00 UTC | Gold lag | Alert hvis SLA brydes |

- [ ] **Oura daily job** — DAB job: kald Oura REST API → parquet → bronze → silver. Brug eksisterende `ingestion_engine.py` + `silver_runner.py`.
- [ ] **Withings daily job** — DAB job: kald Withings REST API → parquet → bronze → silver.
- [ ] **Apple Health weekly job** — DAB job: trigger zip-flow (se nedenfor) → XML → parquet → bronze → silver. Kør mandag.
- [ ] **Lifesum daily job** — DAB job: kald Lifesum scraper/agent (se nedenfor) → parquet → bronze → silver.
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
| Apple Health | < 8 dage | > 100 / uge | E-mail |
| Lifesum | < 25 timer | > 0 / dag | E-mail + webhook |

- [ ] **`gold.data_sla_status`** — tabel med kolonner: `source_system`, `last_ingested_at`, `hours_since_last_ingest`, `sla_hours`, `sla_met`, `actual_rows`, `min_rows_sla`, `rows_sla_met`, `checked_at`.
- [ ] **SLA monitor notebook** — populerer `gold.data_sla_status` og sender alert hvis `sla_met = false`. Brug Databricks notification destinations (e-mail / Slack webhook).
- [ ] **SLA widget i web app** — øverste sektion i dashboard: grøn/rød status per kilde med tidsstempel for sidst opdateret.
- [ ] **Quarantine tracking** — inkludér antal rækker i `bronze.quarantine_*` per kilde i SLA-rapporten. Høj quarantine-rate = datakvalitetsproblem.
- [ ] **Alert kanal** — konfigurér Databricks notification destination + Slack/e-mail webhook i `.env`.
