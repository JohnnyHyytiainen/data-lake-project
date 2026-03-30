# Bronze to silver script - För att
# Kommentarer: Svenska
# Kod: Engelska
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from loguru import logger
import shutil

from config import (
    BRONZE_DIR,
    SILVER_DIR,
    LOG_LEVEL,
    RELEVANT_EVENT_TYPES,
)

# ========== LOGGING ==========
logger.remove()
logger.add(
    sink=lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
)


# ========== DLQ PATH ==========
# Dead Letter Queue bor bredvid silver, inte i silver. Oväntad data har sin egen plats
# så att jag kan inspektera den senare utan att utan att skita ner silver layer
DLQ_DIR = Path("data/dlq/events")


# ========== VALIDERING ==========
# Dessa fält MÅSTE finnas för att event ska vara användbart i analys. Saknas något vet jag inte vad eventet är,
# vem som skapade det, vilket repo det tillhör eller när fan det hände. Utan det här är eventet värdelöst i gold layer.
REQUIRED_FIELDS = {"id", "type", "actor", "repo", "created_at"}


# Priv funktion, fuck around 'n' find out.
def _is_valid(event: dict) -> bool:
    """
    Checks that an event meets the minimum requirements for the Silver layer.

    I check three things in order:
    1. That all required fields are present
    2. That the event type is one I recognize
    3. That created_at can actually be parsed as a date

    If any of these fail, the event belongs in DLQ, not Silver.
    """
    # Kontroll 1: Obligatoriska fält:
    if not REQUIRED_FIELDS.issubset(event.keys()):
        return False
    # Kontroll 2: Känd event type
    if event["type"] not in RELEVANT_EVENT_TYPES:
        return False

    # Kontroll 3: giltig timestamp
    try:
        datetime.fromisoformat(event["created_at"].replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return False

    return True


# ========== Flattenning ==========
# Priv funktion, FAAFO
def _flatten(event: dict) -> dict:
    """
    Flatten a raw event from its nested JSON structure into
    flat columns that PySpark and dbt can work with directly.

    The raw event looks something like this:
    {
    "id": "12345",
    "type": "PushEvent",
    "actor": {"login": "johnnyhyy", "id": 99},
    "repo": {"name": "apache/airflow", "id": 42},
    "payload": {"size": 3, "commits": [...]},
    "created_at": "2026-03-29T16:00:00Z"
    }

    I don't care about everything, I pick out what is actually
    relevant to my Gold questions and give the fields clear names.
    .get() with default value protects me from crashing if an
    optional field happens to be missing.
    """
    return {
        "event_id": event["id"],
        "event_type": event["type"],
        "actor_login": event["actor"].get("login", "unknown"),
        "repo_name": event["repo"].get("name", "unknown"),
        "repo_id": event["repo"].get("id"),
        # Antal commits finns bara i PushEvent, övriga eventtyper får 0
        "commit_count": event.get("payload", {}).get("size", 0),
        # PR-status finns bara i PullRequestEvent
        "pr_action": event.get("payload", {}).get("action"),
        "pr_merged": event.get("payload", {})
        .get("pull_request", {})
        .get("merged", False),
        "created_at": event["created_at"],
    }


# ========== Skriv .parquet ==========
# Priv funktion, FAAFO
def _write_parquet(df: pd.DataFrame, output_dir: Path, label: str) -> None:
    """
    Writes a DataFrame to Parquet with same Hive-style partitioning
    that Brone layer uses. This way Pyspark can read Silver in the exact
    same way as Bronze. Consistent structure throughout the lake.
    """
    if df.empty:
        logger.info(f"No {label} records to write. Skipping..")
        return

    # Parsa datum från created_at för att bygga korrekt partition
    # Använder första radens datum som representant för hela batchen
    sample_dt = datetime.fromisoformat(df["created_at"].iloc[0].replace("Z", "+00:00"))
    partition = (
        f"year={sample_dt.year}/month={sample_dt.month:02d}/day={sample_dt.day:02d}"
    )
    output_path = output_dir / partition
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    output_file = output_path / f"part-{timestamp}.parquet"

    df.to_parquet(output_file, index=False, compression="snappy")
    logger.info(f"Wrote {len(df)} {label} records -> {output_file}")


# ========== Huvudfunktion ==========
def run_bronze_to_silver() -> None:
    """
    Reads all Parquet files from Bronze, transforms them to Silver.

    Flow:
    Read Bronze -> Validate -> Separate valid/invalid ->
    Deduplicate -> Flatten -> Write Silver + DLQ
    """
    logger.info("Starting Bronze -> Silver Transformation")

    # Läser HELA bronze layer i en operation.
    # Pandas förstår hive-style partitioning, den hittar alla .parquet filer under BRONZE_DIR
    # oavsett hur djupt dom ligger
    bronze_files = list(BRONZE_DIR.rglob("*.parquet"))

    if not bronze_files:
        logger.warning(f"No parquet files found in {BRONZE_DIR}. Nothing to transform")
        return

    logger.info(f"Found {len(bronze_files)} parquet files in Bronze")

    # Läs alla filer och slå ihop till EN DataFrame
    # Staplar alla "excel ark" på varandra
    df_raw = pd.concat([pd.read_parquet(f) for f in bronze_files], ignore_index=True)
    logger.info(f"Loaded {len(df_raw)} total events from Bronze layer")

    # ========== VALIDERING: Separera valid från oväntad data ==========
    valid_mask = df_raw.apply(lambda row: _is_valid(row.to_dict()), axis=1)
    df_valid = df_raw[valid_mask].copy()
    df_invalid = df_raw[~valid_mask].copy()

    logger.info(
        f"Validation complete | valid={len(df_valid)} invalid={len(df_invalid)}"
    )

    # Skriv ogiltiga events till DLQ - Bronze layer rörs aldrig!
    if not df_invalid.empty:
        _write_parquet(df_invalid, DLQ_DIR, label="DLQ")

    # ========= Deduplicering ==========
    # Github events API returnerar samma events i flera poll cykler
    # Jag behåller första förekomsten av varje event_id
    before_dedup = len(df_valid)
    df_valid = df_valid.drop_duplicates(subset=["id"], keep="first")
    dupes_removed = before_dedup - len(df_valid)

    if dupes_removed > 0:
        logger.info(f"Removed {dupes_removed} duplicate events")

    # ========== FLATTENNING ==========
    # Konverterar varje rad från nested dicts till platta columns
    df_silver = pd.DataFrame(
        [_flatten(row.to_dict()) for _, row in df_valid.iterrows()]
    )

    # ========== Identifiering av vilka dags partitioner silver output berör ==========
    # Jag läser created_at från den flattade DataFramen och samlar unika dagar.
    # Det är DOM och endast DOM jag ska rensa, INGEN annan silver data rörs!
    partitions_to_clear = set()
    for _, row in df_silver.iterrows():
        dt = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
        partition = SILVER_DIR / f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
        partitions_to_clear.add(partition)

    for partition in partitions_to_clear:
        if partition.exists():
            shutil.rmtree(partition)
            logger.info(f"Cleared Silver partition before rewrite: {partition}")

    # ========== Skriv till silver ==========
    _write_parquet(df_silver, SILVER_DIR, label="Silver")
    logger.info("Bronze -> Silver transformation completed.")


# ===== Entrypoint =====
if __name__ == "__main__":
    run_bronze_to_silver()
