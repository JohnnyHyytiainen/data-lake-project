# Data Lake Project - Roadmap

**Datasource:** GitHub Events API (api.github.com/events)
**Arkitektur:** Medallion (Bronze / Silver / Gold)
**Stack:** Kafka (KRaft) · PySpark · dbt · Airflow · DuckDB · Grafana
**Portfolio-syfte:** Visa och träna DE-kompetens inför LIA januari 2027

---

## Filosofin bakom sekvensen

Varje MVP-fas är designad så att den gör nästa fas bättre, inte bara större. MVP v4 bygger ett fundament av tillförlitlighet som MVP v5's djupare analys kan stå på. 

MVP v5 bygger ett rikt och korrekt Gold layer som MVP v6's frågegränssnitt faktiskt kan ge korrekta svar på.
---

## MVP v1 - Local Medallion Pipeline
*Tagg: `v1.0`*

Mål: En komplett Bronze --> Silver pipeline i Docker med CI och tester.

### Ingestion (Bronze)
- [x] `config.py` - central konfiguration, paths, Kafka-topics, GitHub API-konstanter
- [x] `ingestion/producer.py` - pollar GitHub Events API var 5:e minut, filtrerar på DE_KEYWORDS, skickar till Kafka
- [x] `ingestion/consumer.py` - konsumerar från Kafka, batchar events, skriver Parquet till Bronze med Hive-style partitionering (year=/month=/day=/)
- [x] `docker-compose.yml` - - Kafka (KRaft, ingen ZooKeeper), producer, consumer
- [x] `Dockerfile` - Python 3.12-slim image med uv

### Transform (Silver)
- [x] `transforms/bronze_to_silver.py` - läser Bronze, validerar, deduplicerar på event_id, flattar nästade JSON-fält till Silver-schema
- [x] Idempotens-fix - rensa Silver-partition innan omskrivning så att upprepade körningar ej skapar dubletter
- [x] `tests/test_transforms.py` - initiala unit tests

### CI/CD
- [x] `.github/workflows/ci.yml` - lint (ruff), format (black), pytest vid varje PR
- [x] Main branch skyddad - inga direkta pushes, allt via PR

### Dokumentation
- [x] `docs/architecture/overview_data_lake.mmd` - hela systemet
- [x] `docs/architecture/ingestion.mmd` - Bronze-lagret i detalj
- [x] `docs/architecture/transforms.mmd` - Silver + Gold i detalj
- [x] `docs/visuals/` - komponent-nivå diagram per modul
- [x] `docs/file_docs/` - per-skript dokumentation

---

## MVP v2 - PySpark
*Tagg: `v2.0`*

Mål: Ersätt Pandas med PySpark för Bronze --> Silver, bygg Gold-lagret, kör historisk bootstrap.

### Bootstrap
- [x] `scripts/bootstrap_historical.py` - laddar ner GitHub Archive (.json.gz per timme), packar upp, filtrerar på DE_KEYWORDS, skriver till Bronze. Kritiskt för meningsfulla datamängder.

### Transform (Silver → PySpark)
- [x] Porta `bronze_to_silver.py` från Pandas till PySpark
- [x] Inkrementell läsning med checkpoint-fil (JSON, relativa paths)
- [x] `coalesce(4)` i Silver-skrivsteget - löste I/O-bottleneck (2.5 timmar -> 11 minuter(COLD))

### Transform (Gold)
- [x] `transforms/silver_to_gold.py` - PySpark aggregeringar (CLI-verktyg, primärt ersatt av dbt i v3)

### CLI
- [x] `scripts/run_pipeline.py` - argparse CLI: `--layer bronze|silver|gold|all`

---

## MVP v3 - Orchestration + Serving + dbt  
*Tagg: `v3.0.0`*

Mål: Airflow schemalägger hela pipelinen automatiskt. Grafana visualiserar Gold-lagret.

### dbt
- [x] `dbt/models/staging/stg_github_events.sql` - VIEW, live-fönster mot Silver Parquet
- [x] `dbt/models/marts/tool_growth.sql` - vilka DE-verktyg växer snabbast per vecka?
- [x] `dbt/models/marts/activity_heatmap.sql` - när är DE-communityt aktivt (isodow × timme)?
- [x] `dbt/models/marts/pr_cycle_times.sql` - median + p95 PR-cykeltid per repo
- [x] `Dockerfile.dbt` - containeriserad dbt för Airflow DockerOperator

### Orchestration
- [x] `orchestration/dags/github_lake_dag.py` - Airflow DAG: Bronze --> Silver --> Gold i sekvens
- [x] Airflow + PostgreSQL metadata-DB i `docker-compose.yml`
- [x] `Dockerfile.spark` - custom Spark image med projektets Python-deps

### Serving
- [x] `serving/grafana/dashboards/de_community.json` - 3 paneler: tool_growth, activity_heatmap, pr_cycle_times
- [x] `serving/grafana/provisioning/datasources/duckdb.yml` - uid: github-lake-duckdb
- [x] `serving/grafana/provisioning/dashboards/provider.yml` - automatisk dashboard-provisioning
- [x] Grafana + motherduck-duckdb-datasource v0.4.0 i `docker-compose.yml`

---

## Cleanup Sprint  
*Taggar: `v3.0.1`, `v3.0.2`*

Mål: Stänga all tech-debt från MVP v3 innan v4 påbörjas.

### Infrastruktur-skulden - Checkpoint-filen (v3.0.1)
- [x] `PROJECT_ROOT` som absolut host-path i `.env` - DockerOperator tolkar mount-paths relativt host-maskinen
- [x] `working_dir="/app"` på `run_silver` DockerOperator-tasken
- [x] Checkpoint-paths konverterade till relativa (relativt BRONZE_DIR) - fungerar identiskt på Windows och i Linux-container
- [x] Airflow webserver PID-fil bugg fixad

### Testskulden - PySpark unit tests (v3.0.1)
- [x] `_transform()` extraherad som ren funktion ur `run_bronze_to_silver()` - separation of concerns
- [x] `tests/test_transforms.py` omskriven för PySpark: 12/12 tester gröna
- [x] `pytest` återinförd i `.github/workflows/ci.yml` med Java (Temurin JDK 17)

### Serving layer-skulden (v3.0.2)
- [x] `git clean -fdx`-incident inför Uniplay-intervjun raderade gitignorerade filer (plugin + dashboard-JSON)
- [x] motherduck-duckdb-datasource v0.4.0 installerad - root cause: gamla versioner hade trasig `plugin.json`
- [x] `de_community.json` återställd och committad - skyddad av `.gitignore`-undantag
- [x] Heatmap-etiketter fixade: isodow-korrekt (1=Måndag ... 7=Söndag)
- [x] `.gitignore` inline-kommentar-bugg fixad (inline `#` stöds ej)
- [x] Git-varning om `orchestration/logs/scheduler/latest/` stängd

### Dokumentationsskulden (v3.0.2)
- [x] `docs/architecture/serving.mmd` - Mermaid-diagram med alla arkitekturbeslut inbakade
- [x] `docs/architecture/overview_data_lake.mmd` - DuckDB-nod tillagd i serving-lagret, Gold-nod uppdaterad
- [x] `README.md` - komplett omskrivning: korrekt tech stack, fungerande quickstart, serving layer recovery-sektion, architecture decisions-sektion
- [x] `docs/README.md` - navigationsguide för docs-mappen
- [x] `.gitattributes` - CRLF-normalisering (Windows + Linux cross-platform)
- [x] `ROADMAP.md` - konsoliderad, absorberar PLANNING_v4_v6.md

---

## MVP v4 - Datakvalitet och Pipeline Monitoring
*Mål: Sommaren 2026*

Syfte: Ge pipelinen "åsikter" om sin egen data. Idag producerar systemet resultat utan att veta om de är korrekta. I MVP v4 lägger jag till ett kvalitetslager som aktivt blockerar downstream jobs om Silver-datan inte håller måttet, skillnaden mellan
ett system som tyst kan producera fel och ett system som **vet om** att det producerar fel.

Konceptet är **data contracts**: explicita, maskinläsbara löften om vad datan ska/bör innehålla. Om löftet bryts failar Airflow-tasken synligt istället för att låta dålig data flöda vidare till Gold.

### Nytt DAG-flöde

```
bronze_to_silver --> quality_check_silver --> silver_to_gold_dbt
```

`quality_check_silver` är en ny Airflow-task som kör Soda Core mot Silver Parquet och returnerar pass eller fail. Vid fail stoppas DAGen - `silver_to_gold_dbt` körs aldrig.

### Kvalitetskontrakt att implementera

Volymkontroller (Completeness):
- [x] Antal Silver-rader ≥ 80% av antal Bronze-rader per körning (fångar onormal dataförlust)
- [x] Antal nya Silver-rader > 0 om nya Bronze-filer processades

Null-kontroller på nyckelkolumner:
- [x] `event_id` - aldrig null
- [x] `event_type` - aldrig null
- [x] `repo_name` - aldrig null
- [x] `created_at` - aldrig null

Logiska konsistenskontroller:
- [x] `pr_cycle_time_hours` aldrig negativ (PR kan inte stängas innan den öppnas)
- [x] `pr_action` inte null när `event_type = 'PullRequestEvent'`
- [x] `created_at` inom rimligt intervall (ej i framtiden, ej före 2020)

### Implementation

- [x] Lägg till `soda-core-duckdb` eller `soda-core-spark` i `pyproject.toml`
- [x] Skapa `quality/checks/silver_checks.yml` med kontrakten ovan
- [x] Skapa `quality/run_checks.py` som anropas av Airflow-tasken
- [x] Integrera `quality_check_silver` som ny task i `github_lake_dag.py`
- [x] Logga check-resultat till `data/quality_reports/` för historisk spårbarhet

---

## MVP v4.5 - EDA sprint för djupare insikter kring bronze, silver datan. 

**Setup:**
- [x] Skapa `notebooks/`-mapp
- [x] Lägg till `notebooks/*.ipynb` i `.gitignore`
- [x] Verifiera att `duckdb`, `ipykernel`, `pandas` finns i `pyproject.toml`

**Bronze:**
- [x] Läs en dag av Bronze Parquet, inspektera schema
- [x] Dokumentera payload-struktur per event-typ i Markdown-cell

**Silver:**
- [x] DESCRIBE Silver + verifiera alla kolumner och typer
- [x] Volym per månad (trendgraf)
- [x] Event-typ distribution
- [x] Null-rates per kolumn
- [x] Actor-login bot-mönster analys
- [x] pr_action kontaminations audit

**Dokumentation:**
- [x] Skriv `docs/SILVER_SCHEMA.md`
- [x] Skriv `docs/EDA_FINDINGS.md`

## MVP v5 - Djupare Analys och Bredare Insikter ifrån Githubs data.
*Mål: Sommaren 2026*

Syfte: Nu när pipelinen är tillförlitlig och datakvaliteten är garanterad är det dags att ställa mer intressanta och relevata frågor. MVP v5 expanderar Silver-schemat med klassificerade attribut och bygger nya Gold-modeller.

### Refaktorisera `pr_cycle_times.sql` 
Bug och felande logik hittad och `pr_cycle_times.sql`-mart refactor

- [x] `dbt/models/marts/pr_cycle_times.sql` buggen identifierad och fixad

**Validerat:** `total_rows = distinct_prs` bekräftat via inline dbt-query
(ingen fan-out i joinen)

### Refaktorisera de filer som innehåller `pr_action` --> `event_action` 
Refactor dom script som innehåller `pr_action` och ändra det till `event_action` för att byta namn på `pr_action`-column. Så kallad *Breaking Schema Change*.

- [x] `bronze_to_silver.py`
- [x] `test_transforms.py`
- [x] `silver_to_gold.py`
- [x] `silver_checks.yml`
- [x] `diagnose_soda.py`
- [x] `stg_github_events.sql`
- [x] `pr_cycle_times.sql`


### Bot-klassificering som Silver-kolumn

EDA-observation: många `actor_login`-värden innehåller "bot" - men ett namnbaserat filter är bara den enklaste heuristiken. 

I MVP v5 lägger jag till `is_bot BOOLEAN` som en explicit Silver-kolumn.

**Varför i Silver och inte i Gold?** Klassificeringen är en egenskap hos *aktören* och inte hos en specifik aggregering. Samma `is_bot`-logik används av alla Gold-modeller utan att duplicera logiken - Menat att vara `DRY` fast på datanivå.

Klassificeringskriterier (steg 1 - namnbaserat):
- [x] `actor_login` innehåller `[bot]`, `-bot`, `_bot` (Github Apps-mönster)
- [x] `actor_login` börjar med `dependabot`, `renovate`, `github-actions`
- [x] Dokumentera kriterierna i egna docs **INNAN** kod börjar skrivas.

Möjliga steg 2-kriterier (beteendebaserat):
- Exakt regelbundna tidsintervall mellan events (< 5 sekunders variation)

- Extrem event-volym per timme jämfört med percentil-fördelningen

### Nya Gold-modeller

- [x] `bot_vs_human_activity.sql` - andel events från bots vs humans per event-typ per vecka
- [x] Expandera `DE_KEYWORDS` - Uppdaterat `config.py` med utökad lista av termer, +15st.
- [ ] `repo_health.sql` - sammansatt hälsomått: PR-cykeltider + commit-frekvens + star-tillväxt
- [ ] Expandera `tool_growth.sql` för den nu bredare `DE_KEYWORDS`-listan.

### Bredare search-params

- [ ] Granska `DE_KEYWORDS` i `config.py` och identifiera luckor
- [ ] Enkel precision/recall-validering på sample från utökat keyword-set

---

## MVP v6 - Cloud och RAG
*Mål: Sommaren 2026, synkat med kursen Big Data and Cloud & Data warehouse kurs*

Syfte: Flytta ett produktionsmognat system till cloud-infrastruktur och lägg ett intelligent frågegränssnitt ovanpå Gold-lagret.

*MVP v6 är avsiktligt löst specificerad - detaljer planeras när MVP v5 är levererat och kursen har gett kontext kring cloud-valen.*

### Cloud-migrering

Kurskrav (F15, F16): komplett big-data lösning hos cloud-leverantör med motiverade
lagringsval.

- [ ] Utvärdera Azure Data Lake Storage Gen2 som ersättning för lokal Parquet-storage
- [ ] Undersök self-hosting (Hetzner + MinIO) - relevant för europeisk datasovereignty
- [ ] Dokumentera trade-offs: lock-in, kostnad, latency, compliance, etc

### RAG-lager - "Fråga din data-lake"

Inspirerat av Databricks Genie och erfarenhet från Glossary DB RAG-projektet. Konceptet: naturligt språk --> Text-to-SQL --> DuckDB Gold --> svar i naturligt språk.

- [ ] Definiera scope: vilka frågor ska systemet kunna svara på?
- [ ] FastAPI-lager ovanpå DuckDB (återanvänder mönster från Glossary DB)
- [ ] Chat-gränssnitt eller integration i Grafana-dashboard

---

## Tidslinje

| Fas | Innehåll | Status |
|---|---|---|
| MVP v1 | Bronze -> Silver, Kafka, CI | `v1.0` |
| MVP v2 | PySpark, Gold, bootstrap | `v2.0` |
| MVP v3 | Airflow, dbt, Grafana | `v3.0.0` |
| Cleanup | Tech-debt v3.0.x | `v3.0.1` – `v3.0.2` |
| MVP v4 | Datakvalitet (Soda Core) | Sommaren 2026 |
| MVP v5 | Bot-klassificering, nya Gold-modeller | Sommaren 2026 |
| MVP v6 | Cloud + RAG | Sommaren 2026, synkat med kurs |

---

## Öppna frågor att besvara under resans gång

- Vilken cloud-leverantör ger bäst balans: Azure (kursen), GCP, eller self-hosted MinIO?
- Hur djupt ska RAG-lagret vara - enbart Text-to-SQL, eller även semantisk sökning över commit-meddelanden och PR-beskrivningar?

---

## Bekräftelse kring Examensarbete

Detta projekt kan användas som examensarbete. Mer dokumentation finns tillgänglig.

---

## Kända begränsningar

- GitHub Events API returnerar max 100 events per poll - volymen är låg på helger. Bootstrap med GitHub Archive är primär lösning för meningsfulla datamängder.

- `DE_KEYWORDS`-filtret fångar ibland irrelevanta repos (t.ex fitness-appar med "spark"). Accepterad begränsning för MVP - Fixas till i MVP v5.

- Historisk Bronze-data från mars 2026 saknar `commits` i payload (gammal consumer utan JSON-serialisering). Påverkar `commit_count` i Silver, inte Gold-aggregeringarna.

- `pr_merged`-kolumnen i Silver är alltid `False` - använd `pr_action = 'merged'` för att identifiera merged PRs