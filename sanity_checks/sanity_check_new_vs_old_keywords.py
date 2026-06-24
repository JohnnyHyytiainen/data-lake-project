# Kommentarer: Svenska
# Kod: Engelska
import duckdb

# Rader för ett NYTT keyword som finns FÖRE mitt bootstrap fönstret (16 juni) måste,
# för att ha klarat det gamla filtret vid ingestion, ÄVEN matcha minst ett av de 15 GAMLA keywordsen
# i samma repo_name-sträng. Om det stämmer för (nästan) alla rader -> inget mysterium och off,
# utan bara multi-keyword-repos jag aldrig tänkt på att söka på förut.

OLD_KEYWORDS = [
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

NEW_KEYWORDS = [
    "kestra",
    "apache-beam",
    "apache-arrow",
    "parquet",
    "avro",
    "protobuf",
    "bigquery",
    "airbyte",
    "fivetran",
    "dlt",
    "soda-core",
    "data-contracts",
    "data-lineage",
    "grafana",
    "data-warehouse",
    "data-lakehouse",
]

BOOTSTRAP_WINDOW_START = "2026-06-16"

silver = duckdb.sql("""
    SELECT repo_name, created_at
    FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true)
""")

# Bygger ihop repo_name ILIKE '%dbt%' OR repo_name ILIKE '%airflow%' OR till ett sammansatt SQL condition
# för att återanvända varje nytt keyword här under
old_match_expr = " OR ".join(f"repo_name ILIKE '%{kw}%'" for kw in OLD_KEYWORDS)

print(
    f"{'keyword':<16}{'före fönstret':>14}{'matchar ÄVEN gammalt':>22}{'förklarat %':>13}"
)

for keyword in NEW_KEYWORDS:
    pre_total, pre_also_old = duckdb.sql(f"""
        SELECT
            COUNT(*) AS pre_total,
            COUNT(*) FILTER (WHERE {old_match_expr}) AS pre_also_old
        FROM silver
        WHERE repo_name ILIKE '%{keyword}%'
          AND created_at < '{BOOTSTRAP_WINDOW_START}'
    """).fetchone()

    if pre_total == 0:
        print(f"{keyword:<16}{'0 instängt':>14}")
        continue

    pct = 100 * pre_also_old / pre_total
    print(f"{keyword:<16}{pre_total:>14}{pre_also_old:>22}{pct:>12.1f}%")

    # Om INTE 100% förklarat nu, visa de oförklarade raderna för manuell check av mig
    if pre_also_old < pre_total:
        orphans = duckdb.sql(f"""
            SELECT repo_name, created_at FROM silver
            WHERE repo_name ILIKE '%{keyword}%'
              AND created_at < '{BOOTSTRAP_WINDOW_START}'
              AND NOT ({old_match_expr})
            LIMIT 5
        """).df()
        print(f"  -> {pre_total - pre_also_old} oförklarade rader, stickprov:")
        print(orphans.to_string(index=False))
