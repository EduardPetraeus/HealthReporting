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

## Quality & Testing

- [ ] **dbt tests** — add `schema.yml` with not-null, unique, accepted-values tests per silver entity
- [ ] **Freshness checks** — validate last ingested timestamp per source (bronze → silver lag)
- [ ] **Row count reconciliation** — bronze vs silver row counts after each merge run

## Cleanup

- [ ] **`health_environment/deployment/databricks/`** — catalog/schema DDL scripts (`create_catalog__health_dw.sql`, `create_schemas__health_dw.sql`) may be superseded by `init.py` — consider archiving
