# Config.py
# Kommentarer: Svenska
# Kod: Engelska
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

# Path(__file__) är den här filen. .parent är mappen config ligger i (root)
# Alla data paths byggs relativt till rooot så projektet fungerar oavsett vart på datorn det ligger.
ROOT_DIR = Path(__file__).parent
DATA_DIR = ROOT_DIR / "data"

BRONZE_DIR = DATA_DIR / "bronze" / "events"
SILVER_DIR = DATA_DIR / "silver" / "events"
GOLD_DIR = DATA_DIR / "gold"
# Checkpoint path
CHECKPOINT_DIR = DATA_DIR / "checkpoints"
BRONZE_SILVER_CHECKPOINT = CHECKPOINT_DIR / "bronze_to_silver.json"

# DLQ path
# Dead Letter Queue bor bredvid silver, inte i silver. Oväntad data har sin egen plats
# så att jag kan inspektera den senare utan att utan att skita ner silver layer
DLQ_DIR = DATA_DIR / "dlq" / "events"

# --- Kafka ---
# Producer och consumer importerar härifrån, fristående ifrån varandra
# Pratar till samma topic då båda läser ifrån samma config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC_RAW = "github-events-raw"  # Bronze. Raw data, ej rörd
KAFKA_TOPIC_DLQ = "github-events-dlq"  # Dead letter queue, ogiltiga events
KAFKA_GROUP_ID = "github-lake-consumer-group"

# --- Github API ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # Automatiskt None om ej satt, 60requests/h
GITHUB_EVENTS_URL = "https://api.github.com/events?per_page=100"
POLL_INTERVAL_SEC = 120  # 120 sek mellan varje poll, 2 min.

# -- Event typer jag bryr mig om ---
RELEVANT_EVENT_TYPES = {
    "PushEvent",
    "PullRequestEvent",
    "WatchEvent",
    "ForkEvent",
    "IssueCommentEvent",
    "CreateEvent",
}

# --- Repos/topics som pekar mot att det kan vara DE community ---
# postgresql/mongodb borttagna, risk för volymdominans, generiska databaser
# som används i all mjukvaruutveckling, inte DE-specifikt
DE_KEYWORDS = [
    # Orkestrering & processering - CORE
    "dbt",
    "airflow",
    "spark",
    "pyspark",
    "kafka",
    "flink",
    "dagster",
    "prefect",
    "kestra",
    "apache-beam",
    # Storage + table formats
    "duckdb",
    "delta-lake",
    "iceberg",
    "apache-arrow",
    "parquet",
    "avro",
    "protobuf",
    # Data warehouse - OLAP / OLTP
    "trino",
    "bigquery",
    # Data movement - ELT/ETL
    "airbyte",
    "fivetran",
    "dlt",
    # Data kvalitet & observability
    "soda-core",
    "data-contracts",
    "data-lineage",
    # DataFrames (pandas medvetet exkluderad - kollisionsrisk för ordet)
    "polars",
    # BI / Serving layer
    "grafana",
    # Generella DE-termer
    "data-engineering",
    "data-engineer",
    "data-warehouse",
    "data-lakehouse",
]


# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = ROOT_DIR / "logs"

# --- Partitionering ---
# Använder sig av hive style folder strukturen i data/bronze year/month/day
# Pyspark ska förstå den strukturen utan att behöva ändra någonting eller tänka på nåt specifikt.
DATE_PARTITION_FORMAT = "year={year}/month={month:02d}/day={day:02d}"
