# Bronze to silver script - För att
# Kommentarer: Svenska
# Kod: Engelska
import sys
import os
import json
from pathlib import Path
from datetime import datetime, timezone
from loguru import logger
import shutil

# NYTT: PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

os.environ.setdefault("HADOOP_HOME", r"C:/Program Files/hadoop")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

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


# ========== Silver Schema ==========
# I PySpark definierar jag ett explicit schema.
# PySpark ska inte gissa med vad silver columns ska heta eller ha för typ. Det är mitt kontrakt
# Mot gold layer och det SKA hållas explicit och stabilt.
SILVER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("actor_login", StringType(), nullable=True),
        StructField("repo_name", StringType(), nullable=True),
        StructField("repo_id", StringType(), nullable=True),
        StructField("commit_count", IntegerType(), nullable=True),
        StructField("pr_action", StringType(), nullable=True),
        StructField("pr_merged", BooleanType(), nullable=True),
        StructField("created_at", StringType(), nullable=False),
    ]
)


# ========== Flattening som UDF(user defined functions) ==========
# Med Pyspark använder jag en UDF som Spark anropar parallellt på varje rad i sina distribuerade partitions.
# UDF är som en bro mellan python kod och sparks exekvering.
def _flatten_event(event_id, event_type, actor, repo, payload, created_at):
    """
    Extracts and flattens field from raw event to silver schema.
    Bulletproof version: Will return a safe error row instead of crashing Spark.
    """
    try:
        actor_dict = json.loads(actor) if isinstance(actor, str) else (actor or {})
        repo_dict = json.loads(repo) if isinstance(repo, str) else (repo or {})
        payload_dict = (
            json.loads(payload) if isinstance(payload, str) else (payload or {})
        )

        # Tvinga dem att vara dicts, utifall GitHub skickar en int eller lista av misstag
        if not isinstance(actor_dict, dict):
            actor_dict = {}
        if not isinstance(repo_dict, dict):
            repo_dict = {}
        if not isinstance(payload_dict, dict):
            payload_dict = {}

        pr = payload_dict.get("pull_request") or {}
        if not isinstance(pr, dict):
            pr = {}

        return (
            str(event_id) if event_id is not None else "unknown",
            str(event_type) if event_type is not None else "unknown",
            str(actor_dict.get("login") or "unknown"),
            str(repo_dict.get("name") or "unknown"),
            str(repo_dict.get("id") or ""),
            int(payload_dict.get("size") or 0),  # 'or 0' skyddar mot att size är None
            str(payload_dict.get("action") or ""),
            bool(pr.get("merged") or False),
            str(created_at) if created_at is not None else "unknown",
        )
    except Exception as e:
        # AIRBAG: Om koden ovan kraschar, dö inte!
        # Returnera detta istället, så kan jag se EXAKT vad som gick fel i dataframe sen.
        return (
            str(event_id) if event_id is not None else "error_id",
            "UDF_CRASH",
            "error",
            "error",
            str(e),  # sparar felmeddelandet i repo_id-kolumnen för debugging!
            0,
            "error",
            False,
            str(created_at) if created_at is not None else "unknown",
        )


# ========== Huvudfunktion ==========
def run_bronze_to_silver() -> None:
    """
    Reads Bronze-layer with PySpark, transforms to Silver-layer.

    Flow:
    Read Parquet -> Filter event-types -> Deduplicate ->
    Flatten via UDF -> Clear silver partitions -> Write Silver
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

    # Läser HELA bronze layer i en operation.
    # Pyspark förstår hive-style partitioning, den hittar alla .parquet filer under BRONZE_DIR
    # som Pandas gjorde också oavsett hur djupt dom ligger
    df_bronze = spark.read.parquet(str(BRONZE_DIR))
    total = df_bronze.count()
    # Loggning som ovan
    logger.info(f"Loaded: {total} total events from Bronze Layer")

    # ========== Filtrera på kända event typer ==========
    # Sparks isin() är likadant som Pandas isin() dvs - Filtrerar på en lista
    df_filtered = df_bronze.filter(F.col("type").isin(list(RELEVANT_EVENT_TYPES)))
    logger.info(f"After event-type filter: {df_filtered.count()} events")
    # ========== Deduplicering ==========
    # dropDuplicates() är Sparks version av Pandas drop_duplicates()
    df_deduped = df_filtered.dropDuplicates(["id"])
    dupes = df_filtered.count() - df_deduped.count()
    if dupes > 0:
        logger.info(f"Removed {dupes} duplicate events")

    # ========== Registrera UDF(User Defined Functions) ==========
    # Berättar för Spark vad UDF ska returnera för typ, en struct
    # Som matchar silver schema. Utan det här vet inte spark hur den
    # Ska hantera return values från python funktionen
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        BooleanType,
    )

    udf_return_type = StructType(
        [
            StructField("event_id", StringType()),
            StructField("event_type", StringType()),
            StructField("actor_login", StringType()),
            StructField("repo_name", StringType()),
            StructField("repo_id", StringType()),
            StructField("commit_count", IntegerType()),
            StructField("pr_action", StringType()),
            StructField("pr_merged", BooleanType()),
            StructField("created_at", StringType()),
        ]
    )

    flatten_udf = F.udf(_flatten_event, udf_return_type)

    # Applicera UDF, resultatet är en column med struct-values
    # Jag expanderar structen till separata columns med col("flat.*")
    df_silver = df_deduped.withColumn(
        "flat",
        flatten_udf(
            F.col("id"),
            F.col("type"),
            F.col("actor").cast("string"),
            F.col("repo").cast("string"),
            F.col("payload").cast("string"),
            F.col("created_at"),
        ),
    ).select("flat.*")

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
    # partitionBy() skriver automatiskt till year=/month=/day=/ mapparna
    # baserat på created_at-kolumnen, behöver inte bygga sökvägarna manuellt thank god.
    (
        df_silver.withColumn("year", F.year(F.to_timestamp("created_at")))
        .withColumn("month", F.month(F.to_timestamp("created_at")))
        .withColumn("day", F.dayofmonth(F.to_timestamp("created_at")))
        .write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(str(SILVER_DIR))
    )

    logger.info(f"Wrote {silver_count} Silver records -> {SILVER_DIR}")
    logger.info("Bronze -> Silver transformation complete")

    spark.stop()


if __name__ == "__main__":
    run_bronze_to_silver()
