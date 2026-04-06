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

    # Läser HELA bronze layer i en operation.
    df_bronze = spark.read.parquet(str(BRONZE_DIR))
    total = df_bronze.count()
    logger.info(f"Loaded: {total} total events from Bronze Layer")

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
        .write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(str(SILVER_DIR))
    )

    logger.info(f"Wrote {silver_count} Silver records -> {SILVER_DIR}")
    logger.info("Bronze -> Silver transformation complete")

    spark.stop()


if __name__ == "__main__":
    run_bronze_to_silver()
