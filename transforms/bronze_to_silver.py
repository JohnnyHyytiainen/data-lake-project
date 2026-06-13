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

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Kör enbart på Windows/lokalt, i Docker hanteras Hadoop och Python-paths
# av containerns miljö och image-konfigurationen.
if os.name == "nt":
    os.environ.setdefault("HADOOP_HOME", r"C:/Program Files/hadoop")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from config import (
    BRONZE_DIR,
    SILVER_DIR,
    LOG_LEVEL,
    RELEVANT_EVENT_TYPES,
    BRONZE_SILVER_CHECKPOINT,
)

# ========== LOGGING ==========
logger.remove()
logger.add(
    sink=lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
)


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
    if not BRONZE_SILVER_CHECKPOINT.exists():
        logger.info("No checkpoint found - Will process ALL Bronze files.")
        return set()

    with open(BRONZE_SILVER_CHECKPOINT, "r") as f:
        data = json.load(f)
        relative_paths = set(data.get("processed_files", []))
        processed = {
            str(BRONZE_DIR / Path(rel.replace("\\", "/"))) for rel in relative_paths
        }
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
    BRONZE_SILVER_CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)

    # Konvertera absoluta paths till relativa paths innan lagring
    # Path(f).relative_to(BRONZE_DIR) ger t.ex. year=2025/month=11/day=01/abc.parquet
    relative_paths = [
        Path(f).relative_to(BRONZE_DIR).as_posix() for f in processed_files
    ]

    with open(BRONZE_SILVER_CHECKPOINT, "w") as f:
        json.dump(
            {
                "processed_files": relative_paths,
                "last_run": datetime.now(timezone.utc).isoformat(),
            },
            f,
            indent=2,
        )
    logger.info(f"Checkpoint saved | {len(processed_files)} total processed files")


# ======= Transform funktion =======
# Extraherar transformationslogiken till egen separat funktion.
# Priv funktion, FAAFO
def _transform(df_bronze: DataFrame) -> DataFrame:
    """
    Pure transformation logic: Bronze Dataframe -> Silver dataframe.
    No file-I/O, no checkpoint handling, just transformation.

    SoC in practice.
    """
    # 1) Filtrering på relevanta händelser(events)
    df_filtered = df_bronze.filter(F.col("type").isin(list(RELEVANT_EVENT_TYPES)))
    # 2) Deduplicering för att säkerställa att exakt samma event inte sparas fler ggr
    df_deduped = df_filtered.dropDuplicates(["id"])

    # 3) Flattening med inbyggd Spark-funktioner.
    # Jag skippar UDF:er helt! Inbyggda funktioner extraherar datan direkt
    # i den optimerade JVM-motorn, vilket är mycket snabbare.
    return df_deduped.select(
        F.col("id").cast("string").alias("event_id"),
        F.col("type").cast("string").alias("event_type"),
        # Eftersom att actor och repo sparades som 'structs' i parquet
        # kan jag använda enkel dotnotation direkt istället för json funktioner
        F.col("actor.login").cast("string").alias("actor_login"),
        F.col("repo.name").cast("string").alias("repo_name"),
        F.col("repo.id").cast("string").alias("repo_id"),
        # payload är en raw text string i Bronze, här använder jag get_json_object
        # för att extracta specifika fält. Coalesce för att ge ett default värde (ex 0)
        # om fältet 'size' eller 'number skulle saknas i json-objektet
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
        F.coalesce(
            F.get_json_object(F.col("payload"), "$.pull_request.merged").cast(
                "boolean"
            ),
            F.lit(False),
        ).alias("pr_merged"),
        F.col("created_at").cast("string"),
    )


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
        # 'dynamic' skriver bara över specifika partitioner jag har ny data för
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    # Sätter log nivå till ERROR så Spark inte ska spamma terminalen med WARN
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Starting Bronze -> Silver transformation (PySpark)")

    # ========== INKREMENTELL FIL SELEKTION ==========
    # Hitta ALLA Bronze-filer och jämför med checkpointen för att enbart få ut nya filer.
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

    # ----- TRANSFORMATION -----
    # Anropar min separerade transformerings function
    df_silver = _transform(df_bronze)
    # Trigga cache för att tvinga fram beräkningen, det krävs eftersom
    # count() annars tvingar Spark att beräkna HELA flödet från start igen.
    df_silver.cache()
    silver_count = df_silver.count()
    # Loggning och visuell feedback för att följa silver counts och dedupes+filtrerade (borttagna)
    removed = total - silver_count
    logger.info(
        f"Flattened {silver_count} events to Silver schema | {removed} removed by filter + deduplication"
    )

    # ========== RENSNING AV PARTITIONER  ==========
    # ==========        (Idempotens)      ==========
    # Hittar vilka unika datum min nyligen transformerade data berör
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
        .coalesce(
            4
        )  # Slår ihop data till max 4x filer per partition för bättre I/O prestanda
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
