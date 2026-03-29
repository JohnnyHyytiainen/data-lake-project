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

# --- Kafka ---
# Producer och consumer importerar härifrån, fristående ifrån varandra
# Pratar till samma topic då båda läser ifrån samma config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC_RAW = "github-events-raw"  # Bronze. Raw data, ej rörd
KAFKA_TOPIC_DLQ = "github-events-dlq"  # Dead letter queue, ogiltiga events
KAFKA_GROUP_ID = "github-lake-consumer-group"

# --- Github API ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # Automatiskt None om ej satt, 60requests/h
GITHUB_EVENTS_URL = "https://api.github.com/events"
POLL_INTERVAL_SEC = 300  # 300 sek mellan varje poll, 5 min.

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
DE_KEYWORDS = [
    "dbt",
    "airflow",
    "spark",
    "kafka",
    "flink",
    "dagster",
    "prefect",
    "duckdb",
    "delta-lake",
    "iceberg",
    "trino",
    "pyspark",
    "polars",
    "data-engineering",
    "data-engineer",
]

# --- Logging ---
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = ROOT_DIR / "logs"

# --- Partitionering ---
# Använder sig av hive style folder strukturen i data/bronze year/month/day
# Pyspark ska förstå den strukturen utan att behöva ändra någonting eller tänka på nåt specifikt.
DATE_PARTITION_FORMAT = "year={year}/month={month:02d}/day={day:02d}"
