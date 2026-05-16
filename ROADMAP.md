# Data Lake Project - Roadmap

**Datasource:** GitHub Events API (github.com/timeline)  
**Arkitektur:** Medallion (Bronze / Silver / Gold)  
**Stack:** Kafka (KRaft) · Pandas -> PySpark -> dbt -> Airflow -> Grafana  
**Portfolio-syfte:** Visa och träna DE-kompetens inför LIA 2027

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
- [/] `README.md` - projektbeskrivning, hur man kör lokalt, arkitekturöversikt

---

## MVP v2 — PySpark

Mål: Ersätt Pandas med PySpark för Bronze -> Silver,
bygg Gold-lagret med dbt, kör bootstrap av historisk data.

### Bootstrap
- [x] `scripts/bootstrap_historical.py` - laddar ner GitHub Archive (.json.gz per timme), packar upp, filtrerar på DE_KEYWORDS, skriver direkt till Bronze. **Kritiskt** för att nå volymer där PySpark är **meningsfullt** och inte "over engineered".

### Transform (Silver -> PySpark)
- [x] Porta `bronze_to_silver.py` från Pandas till PySpark
- [x] Lägg till inkrementell läsning - håll koll på vilka Bronze-filer som redan bearbetats

### Transform (Gold)
- [x] `transforms/silver_to_gold.py` - PySpark aggregeringar

### Cli commands (Argparse)
- [x] `scripts/run_pipeline.py` - CLI commands för att kunna köra Silver - Gold layer med enkla commands.

---

## MVP v3 - Orchestration + Serving + dbt

Mål: Airflow schemalägger hela pipelinen automatiskt,
Grafana visualiserar Gold-lagret.

### Docker
- [x] Lägg till PySpark-container i `docker-compose.yml`

### dbt
- [x] `dbt/models/marts/tool_growth.sql` - vilka DE-verktyg (dbt, Airflow, Spark) växer snabbast per vecka?
- [x] `dbt/models/marts/activity_heatmap.sql` - när är DE-communityt aktivt (timme/veckodag)?
- [x] `dbt/models/marts/pr_cycle_times.sql` - hur lång är medel-PR-cykeln i top DE-repos?

### Containerisera dbt
- [x] `docker-compose` - Containerisera dbt innuti docker för att underlätta Airflow DAG.

### Orchestration
- [x] `orchestration/dags/github_lake_dag.py` - Airflow DAG som triggar Bronze -> Silver -> Gold i sekvens
- [x] Lägg till Airflow i `docker-compose.yml`
- [x] Containerisera PostgreSQL databas innuti docker åt `AirFlow` 
- [x] Mounta checkpoint filen i `DockerOperator`-mounts
- [x] Lägg till coalesce i silver skrivningen

### Serving
- [x] `serving/grafana/dashboards/de_community.json` - dashboard med tool_growth, activity_heatmap, pr_cycle_times
- [x] Lägg till Grafana i `docker-compose.yml`

---

## Nästa steg (just nu)
1) - Grafana dashboard **Done**
2) - Containerisera Grafana **Done**
3) - Börja planera på nästa steg. Vad kan vidareutvecklas? (Gå från Airflow(gammalt) -> DAGster(nya) ev. Databricks(Free user, låst och kan bli väldigt begränsande MEN det kanske inte är allt för jobbigt. Jag kan köra det både lokalt via DAGster och rent via Databricks(?))). Hitta några Cloud lösningar(Azure(?)), ta och planera nästkommande steg, lager RAG över det hela(Tänk Genie som du kan ställa frågor för att få svar, den gör queries åt dig på din data). Expandera mina search params(Det jag fiskar efter i projektet. Jag kanske kan bredda det lite mer, inte enbart activity heatmap, PR cycle times eller toolgrowth över tid. Vill kunna se antalet riktiga humans vs bots som gör PRs eller sköter flödet av OS projekt inom data engineering etc)

- Containerisera dbt **DONE**
- Airflow **DONE**
- Containerisera Airflow **DONE**
- Skriva min/mina första DAGs **DONE**
- Mounta checkpoint-filen i DockerOperator-mountsen **DONE MEN BITER EJ ÄNNU**
- Lägg till coalesce i Silver-skrivsteget **DONE**

---

## Kända begränsningar (dokumenterade)

GitHub Events API returnerar max 100 events per poll och filtrerar hårt på DE_KEYWORDS - volymen är låg på helger.  
Bootstrap med Github Archive är den primära lösningen för meningsfulla datamängder. Mountad checkpoint-fil via DockerOperator biter inte riktigt ännu vid körning av DAG. Ej stort problem just nu men bör ses över och fixas innan MVP v4 påbörjas.
