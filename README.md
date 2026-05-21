# GitHub Data Lake (Medallion Architecture)

> **Personal project - Building a scalable data lake to master the data lifecycle, data engineering, and the Medallion architecture.**
![Project overview](docs/architecture/overview_data_lake.png)

## Project Purpose & Business Value

To track and analyze trends within Data Engineering tools, this pipeline extracts actionable insights from the "firehose" of GitHub live events. The project demonstrates a complete end-to-end pipeline: from streaming raw data (Kafka) to validated historical storage (Parquet), and finally into analytical views (dbt/PySpark).

## Project Structure & MVP Roadmap

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
│   └── consumer.py                       # Kafka → Bronze (Parquet on disk)
│
├── transforms/
│   ├── __init__.py
│   ├── bronze_to_silver.py               # PySpark: Raw data → Validated
│   └── silver_to_gold.py                 # PySpark: Validated → Aggregated
│
├── dbt/                                  # MVP v3
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   └── stg_github_events.sql
│       └── marts/
│           ├── tool_growth.sql           # Which DE tools are growing the fastest?
│           ├── activity_heatmap.sql      # When is the community most active?
│           └── pr_cycle_times.sql        # What is the median PR cycle time?
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
├── data/                                 # Gitignored entirely (see .gitignore)
│   ├── bronze/
│   │   └── events/
│   │       └── year=2025/
│   │           └── month=01/
│   │               └── day=15/           # Hive-style partitioning
│   │                   └── *.parquet
│   ├── silver/
│   │   └── events/
│   └── gold/
│       ├── tool_growth/
│       ├── activity_heatmap/
│       └── pr_cycle_times/
│
├── scripts/
│   ├── bootstrap_historical.py           # GH Archive → Bronze (One-off run)
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
│   │   ├── overview.mmd                  # Complete System Architecture
│   │   ├── ingestion.mmd                 # Bronze Layer
│   │   ├── transforms.mmd                # Silver + Gold Layers
│   │   └── serving.mmd                   # Grafana Layer
│   └── session_tracking/                 # Learning logs per session
│       └── session_00x.md
│
├── docker-compose.yml                    # Kafka + KRaft + Spark (+ Airflow)
├── .env                                  # Gitignored
├── .env.example                          # Committed (no actual secrets)
├── .gitignore
├── config.py                             # Central config (topics, paths, constants)
├── pyproject.toml                        # Dependency management via `uv`
└── README.md

```

[View Detailed Roadmap](ROADMAP.md)

## Tech Stack

* **Language:** Python 3.12 (managed via `uv`)
* **Ingestion:** Apache Kafka (KRaft) & GitHub REST API
* **Processing & Transformation:** Pandas, PySpark, dbt
* **Storage:** Local Parquet files (Hive-partitioned)
* **DevOps & Quality:** Docker Compose, GitHub Actions (CI), Ruff, Pytest

## Quickstart (Run Locally)

1. Clone the repository and copy `.env.example` to `.env`
2. Run `uv sync` to build the environment
3. Spin up the Kafka cluster using `docker compose up -d`

### Grafana DuckDB Plugin (Manual Installation Required)

The `motherduck-duckdb-datasource` plugin is not available in Grafana's official registry and is omitted from this repository (binary file, ~300MB). To install it manually:

1. Navigate to [https://github.com/motherduckdb/grafana-duckdb-datasource/releases/tag/v0.4.0](https://github.com/motherduckdb/grafana-duckdb-datasource/releases/tag/v0.4.0)
2. Download `motherduck-duckdb-datasource-v0.4.0.linux_amd64.zip`
3. Extract the contents to `serving/grafana/plugins/motherduck-duckdb-datasource/`

---

## TODO: Update README and ROADMAP.md as I continue development on this project. For now, click here for [current iteration of complete MVP v1-v3 ROADMAP](ROADMAP.md) or [click here for current planning phase for upcoming MVP versions](docs/session_tracking/PLANNING_v4_v6.md). *note: all documents in this repo needs to be translated from Swedish to English*