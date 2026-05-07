# Bronze to silver script - För att
# Kommentarer: Svenska
# Kod: Engelska
from __future__ import annotations
import sys
import os
from pathlib import Path
from loguru import logger
import shutil
import json
from datetime import datetime, timezone

# NYTT: PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ.setdefault("HADOOP_HOME", r"C:/Program Files/hadoop")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from config import (
    BRONZE_DIR,
    SILVER_DIR,
    LOG_LEVEL,
    RELEVANT_EVENT_TYPES,
)

# ==== CHECKPOINT-HANTERING ===
# Checkpoint filen ska leva utanför data folders så den aldrig råkar rensas
# av shutil.rmtree när jag rensar silver-partitions
CHECKPOINT_FILE = Path("data/checkpoints/bronze_to_silver.json")

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


# ========== CHECKPOINT FUNKTIONER ==========
def _load_checkpoint() -> set[str]:
    """
    Reads the checkpoint file and returns an array of already processed
    file paths. Returns an empty array if the file does not yet exist
    this is expected behavior on first run.

    Uses a set (array) instead of a list for an important
    reason: checking if a file has already been processed is O(1)
    with a set, versus O(n) with a list. When you have thousands
    of checkpointed files, this matters a lot.
    """
    if not CHECKPOINT_FILE.exists():
        logger.info("No checkpoint found - Will process ALL Bronze files.")
        return set()

    with open(CHECKPOINT_FILE, "r") as f:
        data = json.load(f)
        processed = set(data.get("processed_files", []))
        last_run = data.get("last_run", "unknown")
        logger.info(
            f"Checkpoint loaded | {len(processed)} files already processed | last_run={last_run}"
        )
        return processed


def _save_checkpoint(processed_files: set[str]) -> None:
    """
    Writes an updated checkpoint file to disk after a successful run.
    I ALWAYS write the checkpoint after the Silver data is safely on disk,
    never before. Same principle as offset-commit in consumer.py:
    data to disk is always priority one.
    """
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(
            {
                "processed_files": list(processed_files),
                "last_run": datetime.now(timezone.utc).isoformat(),
            },
            f,
            indent=2,
        )
    logger.info(f"Checkpoint saved | {len(processed_files)} total processed files")


# ========== Huvudfunktion ==========
def run_bronze_to_silver() -> None:
    """
    Reads Bronze-layer with PySpark, transforms to Silver-layer.

    Flow:
    Read Parquet -> Filter event-types -> Deduplicate ->
    Flatten via native Spark -> Clear silver partitions -> Write Silver
    """
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("github-data-lake-bronze-to-silver")
        # Talar om för Spark att skriva Parquet med Hive-style partitionering
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    # Sätter log nivå till ERROR så Spark inte ska spamma terminalen med WARN
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Starting Bronze -> Silver transformation (PySpark)")

    # ========== INKREMENTELL FIL SELEKTION ==========
    # Hitta ALLA Bronze filer och filtrera bort de jag redan bearbetat.
    all_bronze_files = [str(p) for p in BRONZE_DIR.rglob("*.parquet")]
    processed_files = _load_checkpoint()

    new_files = [f for f in all_bronze_files if f not in processed_files]

    if not new_files:
        logger.info("No new Bronze files since last run - Nothing to do!")
        spark.stop()
        return

    logger.info(
        f"Found {len(all_bronze_files)} total Bronze files | "
        f"{len(processed_files)} already processed | "
        f"{len(new_files)} new files to process"
    )

    # Läs BARA in de NYA filerna, INTE hela Bronze layer.
    df_bronze = spark.read.parquet(*new_files)
    total = df_bronze.count()
    logger.info(f"Loaded {total} new events from Bronze layer.")

    # ========== Filtrera på kända event typer ==========
    df_filtered = df_bronze.filter(F.col("type").isin(list(RELEVANT_EVENT_TYPES)))
    logger.info(f"After event-type filter: {df_filtered.count()} events")

    # ========== Deduplicering ==========
    df_deduped = df_filtered.dropDuplicates(["id"])
    dupes = df_filtered.count() - df_deduped.count()
    if dupes > 0:
        logger.info(f"Removed {dupes} duplicate events")

    # ========== Flattening med inbyggda Spark-funktioner ==========
    # Jag skippar UDFen helt! get_json_object extraherar data direkt i JVM-motorn.
    df_silver = df_deduped.select(
        F.col("id").cast("string").alias("event_id"),
        F.col("type").cast("string").alias("event_type"),
        # 1) actor och repo sparades som 'Structs' (nästlade objekt) i Parquet.
        # Därför kan jag använda punkt notation direkt! Inget behov av json-funktioner.
        F.col("actor.login").cast("string").alias("actor_login"),
        F.col("repo.name").cast("string").alias("repo_name"),
        F.col("repo.id").cast("string").alias("repo_id"),
        # 2) payload sparades som en raw textsträng i Bronze.
        # Jag behåller därför get_json_object här för att gräva i str
        # Använd coalesce för att fylla på med 0 om "size" saknas
        F.coalesce(
            F.get_json_object(F.col("payload"), "$.size").cast("integer"), F.lit(0)
        ).alias("commit_count"),
        F.coalesce(
            F.get_json_object(F.col("payload"), "$.pull_request.number").cast(
                "integer"
            ),
            F.lit(0),
        ).alias("pr_number"),
        F.get_json_object(F.col("payload"), "$.action").alias("pr_action"),
        # Coalesce för att ge default False om "merged" saknas
        F.coalesce(
            F.get_json_object(F.col("payload"), "$.pull_request.merged").cast(
                "boolean"
            ),
            F.lit(False),
        ).alias("pr_merged"),
        F.col("created_at").cast("string"),
    )

    # Trigga cache för att tvinga fram beräkningen
    df_silver.cache()
    silver_count = df_silver.count()
    logger.info(f"Flattened {silver_count} events to Silver schema")

    # ========== Rensar berörda silver partitions ==========
    # ==========        (Idempotens)              ==========
    dates = df_silver.select(F.to_date("created_at").alias("date")).distinct().collect()

    for row in dates:
        dt = row["date"]
        partition = SILVER_DIR / f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
        if partition.exists():
            shutil.rmtree(partition)
            logger.info(f"Cleared Silver partition: {partition}")

    # ========== Skriva till silver ==========
    (
        df_silver.withColumn("year", F.year(F.to_timestamp("created_at")))
        # Date_format med "MM" och "dd" tvingar fram inledande nollor, ex: month=03
        .withColumn("month", F.date_format(F.to_timestamp("created_at"), "MM"))
        .withColumn("day", F.date_format(F.to_timestamp("created_at"), "dd"))
        .coalesce(4)
        .write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(str(SILVER_DIR))
    )

    logger.info(f"Wrote {silver_count} Silver records -> {SILVER_DIR}")

    # ===== Spara checkpoint EFTER att Silver är säkert på disk =====
    # Om jag sparade checkpointen INNAN skrivningen och sedan kraschade
    # hade systemet lurats att tro att filerna är bearbetade men
    # Silver-datan skulle saknas. Data till disk alltid prio 1, 2 och 3.
    updated_processed = processed_files | set(new_files)
    _save_checkpoint(updated_processed)

    logger.info("Bronze -> Silver transformation complete")

    spark.stop()


if __name__ == "__main__":
    run_bronze_to_silver()
