# Data Lake Project - Roadmap

**Datasource:** GitHub Events API (github.com/timeline)  
**Arkitektur:** Medallion (Bronze / Silver / Gold)  
**Stack:** Kafka (KRaft) · Pandas -> PySpark -> dbt -> Airflow -> Grafana  
**Portfolio-syfte:** Visa och träna DE-kompetens inför LIA @ CGI Jan 2027(HOPEFULLY)

---

## MVP v1 - Local Medallion Pipeline (Parquet på disk)

Mål: En komplett Bronze -> Silver pipeline som körs i Docker,
med CI och tester. Ingen extern databas, all storage är Parquet-filer.

### Ingestion (Bronze)
- [x] `config.py` - central konfiguration, paths, Kafka-topics, GitHub API-konstanter
- [x] `ingestion/producer.py` - pollar GitHub Events API varje 2 minuter, filtrerar på DE_KEYWORDS, skickar till Kafka
- [x] `ingestion/consumer.py` - konsumerar från Kafka, batchar events, skriver Parquet till Bronze med Hive-style partitionering (year=/month=/day=/)
- [x] `docker-compose.yml` - Kafka (KRaft, ingen Zookeeper), producer, consumer
- [x] `Dockerfile` - tunn Python 3.12-slim image med uv

### Transform (Silver)
- [x] `transforms/bronze_to_silver.py` - läser Bronze, validerar, deduplicerar på event_id, flattar nästade JSON-fält till Silver-schema
- [x] **Idempotens-fix** - rensa Silver-partition innan omskrivning (shutil.rmtree) så att upprepade körningar ej skapar dubletter
- [x] `tests/test_transforms.py` - 13/13 unit tests för _is_valid() och _flatten()

### CI/CD
- [x] `.github/workflows/ci.yml` - lint (ruff), format (black), pytest (13 tester) vid varje PR
- [x] Main branch skyddad - inga direkta pushes, allt via PR

### Dokumentation
- [x] `docs/architecture/overview.mmd` - hela systemet
- [x] `docs/architecture/ingestion.mmd` - Bronze i detalj
- [x] `docs/architecture/transforms.mmd` - Silver + Gold i detalj
- [x] `docs/file_docs/` - egna docs för varje script som fylls i med tiden
- [x] `docs/visuals/` - egna och enklare visuals för att förstå moduler och flöde
- [ ] `README.md` - projektbeskrivning, hur man kör lokalt, arkitekturöversikt

---

## MVP v2 — PySpark + dbt

Mål: Ersätt Pandas med PySpark för Bronze -> Silver,
bygg Gold-lagret med dbt, kör bootstrap av historisk data.

### Bootstrap
- [ ] `scripts/bootstrap_historical.py` - laddar ner GitHub Archive (.json.gz per timme), packar upp, filtrerar på DE_KEYWORDS, skriver direkt till Bronze. **Kritiskt** för att nå volymer där PySpark är **meningsfullt** och inte "over engineered".

### Transform (Silver -> PySpark)
- [ ] Porta `bronze_to_silver.py` från Pandas till PySpark
- [ ] Lägg till inkrementell läsning - håll koll på vilka Bronze-filer som redan bearbetats

### Transform (Gold)
- [ ] `transforms/silver_to_gold.py` - PySpark aggregeringar
- [ ] `dbt/models/marts/tool_growth.sql` - vilka DE-verktyg (dbt, Airflow, Spark) växer snabbast per vecka?
- [ ] `dbt/models/marts/activity_heatmap.sql` - när är DE-communityt aktivt (timme/veckodag)?
- [ ] `dbt/models/marts/pr_cycle_times.sql` - hur lång är medel-PR-cykeln i top DE-repos?

### Docker
- [ ] Lägg till PySpark-container i `docker-compose.yml`

---

## MVP v3 - Orchestration + Serving

Mål: Airflow schemalägger hela pipelinen automatiskt,
Grafana visualiserar Gold-lagret.

### Orchestration
- [ ] `orchestration/dags/github_lake_dag.py` - Airflow DAG som triggar Bronze -> Silver -> Gold i sekvens
- [ ] Lägg till Airflow i `docker-compose.yml`

### Serving
- [ ] `serving/grafana/dashboards/de_community.json` - dashboard med tool_growth, activity_heatmap, pr_cycle_times
- [ ] Lägg till Grafana i `docker-compose.yml`

---

## Nästa steg (just nu)

1. Idempotensfix i `bronze_to_silver.py` (MVP v1, sista öppna punkten)
2. `README.md` grundstruktur
3. Grena ut till `feat/silver-to-gold` och börja MVP v2

---

## Kända begränsningar (dokumenterade)

GitHub Events API returnerar max 100 events per poll och filtrerar
hårt på DE_KEYWORDS - volymen är låg på helger. Bootstrap med
GitHub Archive är den primära lösningen för meningsfulla datamängder.

Silver-lagret är ännu inte idempotent - upprepade körningar
skapar överlappande Parquet-filer tills idempotens lösningen är på plats.