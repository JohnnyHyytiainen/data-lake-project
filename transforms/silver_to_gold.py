# Silver to gold script för att aggregera silver data till gold layer(berika datan för analys)
# Kommentarer: Svenska
# Kod: Engelska
import shutil
from pathlib import Path
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import sys

os.environ.setdefault("HADOOP_HOME", r"C:/Program Files/hadoop")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from config import SILVER_DIR, LOG_LEVEL

# ========== LOGGING ==========
logger.remove()
logger.add(
    sink=lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
)

# ===== Gold layers tre tables får egna mappar under data/gold folders =====
GOLD_DIR = Path("data/gold")
TOOL_GROWTH = GOLD_DIR / "tool_growth"
ACTIVITY_MAP = GOLD_DIR / "activity_heatmap"
PR_CYCLES = GOLD_DIR / "pr_cycle_times"


# ===== Helper functions: Rensa och skriv Gold partitions =====
# Priv funktion FAFO
def _write_gold(df, output_dir: Path, label: str) -> None:
    """
    Clears existing Gold table and writes a new version.
    Gold tables, unlike Silver, are not partitioned by day,
    they are aggregated across the entire dataset and written as a single table.
    Therefore, I clear the entire output_dir instead of individual day partitions.
    """
    if output_dir.exists():
        shutil.rmtree(output_dir)
        logger.info(f"Cleared Existing {label} table.")

    df.write.mode("overwrite").parquet(str(output_dir))
    logger.info(f"Wrote {label} -> {output_dir}")


# ========== GOLD 1: tool_growth ==========
# Se trender i verktygen jag letar efter ifrån DE community på Github
def build_tool_growth(df_silver) -> None:
    """
    Answers: which DE tools are growing the fastest in terms of stars per week?

    Filtering on WatchEvent (= a star on GitHub) and ForkEvent,
    group by repo and week, and count the number of events.
    date_trunc("week", ...) rounds a date down to Monday of the same week,
    this makes all days of the same week get the same week key,
    which is exactly what I want for a weekly aggregation.
    """
    logger.info("Building tool_growth..")

    df = (
        df_silver.filter(F.col("event_type").isin("WatchEvent", "ForkEvent"))
        # Konverterar created_at str till timestamp så att date_trunc fungerar
        .withColumn("ts", F.to_timestamp("created_at"))
        .withColumn("week", F.date_trunc("week", F.col("ts")))
        .groupBy("repo_name", "week", "event_type")
        .agg(F.count("*").alias("event_count"))
        # Pivot: gör om event_type-rader till kolumner (stars, forks)
        # Innan pivot:  repo | week | event_type | count
        # Efter pivot:  repo | week | stars      | forks
        .groupBy("repo_name", "week")
        .pivot("event_type", ["WatchEvent", "ForkEvent"])
        .agg(F.first("event_count"))
        .withColumnRenamed("WatchEvent", "stars")
        .withColumnRenamed("ForkEvent", "forks")
        # Fyll null med 0, om en vecka hade stars men inga forks blir forks null
        .fillna(0, subset=["stars", "forks"])
        .orderBy("repo_name", "week")
    )

    _write_gold(df, TOOL_GROWTH, "tool_growth")


# ========== GOLD 2: activity_heatmap ==========
def build_activity_heatmap(df_silver) -> None:
    """
    Answering: when is the DE community active?

    Im extracting the hour (0-23) and day of the week (1=Sunday, 7=Saturday in Spark)
    from created_at and count the total number of events per combination.
    The result is a 7x24 matrix, a heatmap, that shows which
    hours and days are most active. It is this table
    that hopefully produces the visually interesting pattern in Grafana later.
    """
    logger.info("Building activity_heatmap..")

    df = (
        df_silver.withColumn("ts", F.to_timestamp("created_at"))
        # hour() extraherar timmen (0-23) från en timestamp
        .withColumn("hour_of_day", F.hour("ts"))
        # dayofweek() ger 1=Söndag, 2=Måndag ... 7=Lördag
        .withColumn("day_of_week", F.dayofweek("ts"))
        # date_format ger mig läsbara namn: "Monday", "Tuesday" etc
        .withColumn("day_name", F.date_format("ts", "EEEE"))
        .groupBy("hour_of_day", "day_of_week", "day_name")
        .agg(F.count("*").alias("event_count"))
        .orderBy("day_of_week", "hour_of_day")
    )

    _write_gold(df, ACTIVITY_MAP, "activity_heatmap")


# ========== Gold 3: pr_cycle_times ==========
def build_pr_cycle_times(df_silver) -> None:
    """
    Answers: how long is a typical PR cycle in DE repos?

    This is the most complex aggregation, a self-join.
    Silver has separate rows for opened and closed PRs.
    I need to pair them together to calculate the time difference.
    """
    logger.info("Building pr_cycle_times..")

    # Filtrerar bara fram PullRequestEvents
    df_pr = df_silver.filter(F.col("event_type") == "PullRequestEvent").withColumn(
        "ts", F.to_timestamp("created_at")
    )

    # Skapa TVÅ separata views av samma data, kärnan i self JOINS.
    # alias() ger varje view ett unikt namn så att Spark kan skilja dom åt när de joinas.
    # Utan alias vet inte Spark vilket 'repo_name' jag menar i scriptet.
    df_opened = (
        df_pr.filter(F.col("pr_action") == "opened")
        .select(
            F.col("repo_name"),
            F.col("pr_number"),  # <--- NY EFTER CARTESIAN PRODUCT TABBEN
            F.col("ts").alias("opened_at"),
        )
        .alias("opened")
    )

    df_closed = (
        df_pr.filter((F.col("pr_action") == "closed") & F.col("pr_merged"))
        .select(
            F.col("repo_name"),
            F.col("pr_number"),  # <---- NY EFTER CARTESIAN PRODUCT TABBEN
            F.col("ts").alias("closed_at"),
        )
        .alias("closed")
    )

    # Self JOIN. Para ihop opened med closed på samma repo. Lägg till villkoret att closed måste vara EFTER opened
    # OCH att skillnaden är "rimlig" < 30 dagar == 2 593 000 sec.
    df_joined = (
        df_opened.join(df_closed, on=["repo_name", "pr_number"], how="inner")
        .withColumn(
            "cycle_hours",
            (F.unix_timestamp("closed_at") - F.unix_timestamp("opened_at")) / 3600,
        )
        .filter(F.col("cycle_hours").between(0, 720))
    )

    # Aggregerar per repo. Median och 95th percentile för cykeltider.
    # percentile_approx == Sparks inbyggda funktion för att beräkna 'approximate percentile of a numeric column in a large dataset'
    # Exakt percentil är inte rimlig att beräkna vid stora dataset..
    df_gold = (
        df_joined.groupBy("repo_name")
        .agg(
            F.count("*").alias("pr_count"),
            F.round(F.percentile_approx("cycle_hours", 0.5), 1).alias("median_hours"),
            F.round(F.percentile_approx("cycle_hours", 0.95), 1).alias("p95_hours"),
        )
        # Filtrera bort repos med väldigt få PRs som inte ger meningsfull statistik
        .filter(F.col("pr_count") >= 5)
        .orderBy(F.col("median_hours").asc())
    )

    _write_gold(df_gold, PR_CYCLES, "pr_cycle_times")


# ========== HUVUDFUNKTION ==========
# Läser silver en gång och återanvänd för alla tre aggregations.
# cache() håller kvar min DF i minnet så jag inte behöver läsa från disk 3x och inte behöver bry mig om OOM issues what so ever!
def run_silver_to_gold() -> None:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("github-data-lake-silver-to-gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Starting Silver to Gold transformation with PySpark")

    df_silver = spark.read.parquet(str(SILVER_DIR)).cache()
    total = df_silver.count()
    logger.info(f"Loaded: {total} Silver records!")

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    build_tool_growth(df_silver)
    build_activity_heatmap(df_silver)
    build_pr_cycle_times(df_silver)

    logger.info("Silver to Gold transformation is complete.")
    spark.stop()


if __name__ == "__main__":
    run_silver_to_gold()
