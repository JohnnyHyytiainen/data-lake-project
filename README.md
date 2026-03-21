# data-lake-project
Personal project - Build my own data lake to further deepen understanding and knowledge about data lakes, data lifecycle and data engineering.

![Project overview](docs/architecture/overview_data_lake.png)

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
├── dbt/                                  # MVP v2
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
│       └── session_001.md
│
├── docker-compose.yml                    # Kafka + Zookeeper (+ senare Airflow)
├── .env                                  # Gitignorerad
├── .env.example                          # Committad (inga riktiga värden)
├── .gitignore
├── config.py                             # Central config (topics, paths, konstanter)
├── pyproject.toml                        # uv hanterar deps
└── README.md
```