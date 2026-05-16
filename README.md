# GitHub Data Lake (Medallion Architecture)

> **Personal project - Building a scalable data lake to master the data lifecycle, data engineering, and the Medallion architecture.**
![Project overview](docs/architecture/overview_data_lake.png)

## Projektets Syfte & Affärsvärde
För att spåra trender inom Data Engineering-verktyg bygger denna pipeline insikter från GitHubs "brandslang av live-events". Projektet demonstrerar en end-to-end pipeline från rådata (Kafka) till validerad historik (Parquet) och analytiska vyer (dbt/PySpark).


## Projektstruktur & MVP Roadmap
```text
github-data-lake/
│
├── .github/
│   └── workflows/
│       └── ci.yml                        # GitHub Actions CI/CD (MVP v2+)
│
├── ingestion/
│   ├── __init__.py
│   ├── producer.py                       # GitHub API → Kafka topic
│   └── consumer.py                       # Kafka → Bronze (Parquet på disk)
│
├── transforms/
│   ├── __init__.py
│   ├── bronze_to_silver.py               # PySpark: rådata → validerad
│   └── silver_to_gold.py                 # PySpark: validerad → aggregerad
│
├── dbt/                                  # MVP v3
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   └── stg_github_events.sql
│       └── marts/
│           ├── tool_growth.sql           # Vilka DE-verktyg växer snabbast?
│           ├── activity_heatmap.sql      # När är communityt aktivt?
│           └── pr_cycle_times.sql        # Hur lång är en typisk PR-cykel?
│
├── orchestration/                        # MVP v3
│   └── dags/
│       └── github_lake_dag.py            # Airflow DAG
│
├── serving/                              # MVP v3
│   └── grafana/
│       └── dashboards/
│           └── de_community.json
│
├── data/                                 # Gitignorerad i helhet (se .gitignore)
│   ├── bronze/
│   │   └── events/
│   │       └── year=2025/
│   │           └── month=01/
│   │               └── day=15/           # Hive-style partitionering
│   │                   └── *.parquet
│   ├── silver/
│   │   └── events/
│   └── gold/
│       ├── tool_growth/
│       ├── activity_heatmap/
│       └── pr_cycle_times/
│
├── scripts/
│   ├── bootstrap_historical.py           # GH Archive → Bronze (engångskörning)
│   └── run_pipeline.py                   # argparse CLI: --layer bronze|silver|gold|all
│
├── tests/
│   ├── __init__.py
│   ├── test_producer.py
│   ├── test_consumer.py
│   └── test_transforms.py
│
├── docs/
│   ├── architecture/
│   │   ├── overview.mmd                  # Hela systemet
│   │   ├── ingestion.mmd                 # Bronze-lagret
│   │   ├── transforms.mmd                # Silver + Gold
│   │   └── serving.mmd                   # Grafana-lagret
│   └── session_tracking/                 # Lärloggar per session
│       └── session_00x.md
│
├── docker-compose.yml                    # Kafka + KRaft + Spark (+ senare Airflow)
├── .env                                  # Gitignorerad
├── .env.example                          # Committad (inga riktiga värden)
├── .gitignore
├── config.py                             # Central config (topics, paths, konstanter)
├── pyproject.toml                        # uv hanterar deps
└── README.md
```
[Mer detaljerad Roadmap](ROADMAP.md)


## Tech Stack:
* **Språk:** Python 3.12 (hanterat via `uv`)
* **Ingestion:** Apache Kafka (KRaft) & GitHub REST API
* **Processing & Transformation:** Pandas, PySpark, dbt
* **Storage:** Lokala Parquet-filer (Hive-partitionering)
* **DevOps & Kvalitet:** Docker Compose, GitHub Actions (CI), Ruff, Pytest


## Quickstart (Kör lokalt)
1. Klon repot och kopiera `.env.example` till `.env`
2. Kör `uv sync` för att bygga miljön
3. Snurra upp Kafka-klustret med `docker compose up -d`

### Grafana DuckDB Plugin (manuell installation krävs)

Pluginet `motherduck-duckdb-datasource` finns inte i Grafanas officiella
registry och ingår inte i repot (binärfil, ~300MB). Ladda ner det manuellt:

1. Gå till https://github.com/motherduckdb/grafana-duckdb-datasource/releases/tag/v0.4.0
2. Ladda ner `motherduck-duckdb-datasource-v0.4.0.linux_amd64.zip`
3. Packa upp till `serving/grafana/plugins/motherduck-duckdb-datasource/`

## TODO: Fylla i resten av README.md