# Kommentarer: Svenska
# Kod: Engelska
import duckdb
import pandas as pd

# Sanity check för de 16 NYA DE_KEYWORDS termerna
# Två checks:
# 1. Stickprov repo_names, ögon-koll, samma sak som is_bot-valideringen.
# 2. Datumintervall, ett nytt keyword SKA klustra inom 16-24 juni.
# Träffar utanför fönstret är arkitektoniskt oväntade (gammal Bronze filtrerades aldrig mot specifikt ord)


# Lista med alla NYA keywords
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

# Läs silver EN gång, 'silver' blir en DuckDB relation som duckdb.sql() automatiskt känner igen i
# f-string nedan (DuckDBs "replacement scan")
silver = duckdb.sql("""
    SELECT repo_name, created_at
    FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true)
""")


# Lista för summering att fylla på
summary_rows = []


# ILIKE = case insensitive, speglar .lower() matchning i producer/bootstrap.py script
# Valideringen ska testa exakt SAMMA regler som produktionskoden och inte en egen tolkning av den
for keyword in NEW_KEYWORDS:
    match_count, distinct_repos, first_seen, last_seen = duckdb.sql(f"""
        SELECT
            COUNT(*) AS match_count,
            COUNT(DISTINCT repo_name) AS distinct_repos,
            MIN(created_at) AS first_seen,
            MAX(created_at) AS last_seen
        FROM silver
        WHERE repo_name ILIKE '%{keyword}%'
    """).fetchone()

    summary_rows.append(
        {
            "keyword": keyword,
            "match_count": match_count,
            "distinct_repos": distinct_repos,
            "first_seen": first_seen,
            "last_seen": last_seen,
        }
    )

    if match_count > 0:
        sample = duckdb.sql(f"""
            SELECT DISTINCT repo_name FROM silver
            WHERE repo_name ILIKE '%{keyword}%' LIMIT 5
        """).df()
        print(f"\n===== {keyword} ({match_count} rader, {distinct_repos} repos) ====")
        print(f"Datumintervall: {first_seen} --> {last_seen}")
        print(sample.to_string(index=False))
    else:
        print(f"\n=== {keyword} (0 träffar) ===")

# Sorterad overview, en outlier i match_count ska synas direkt och är en varning för volume dominance
summary_df = pd.DataFrame(summary_rows).sort_values("match_count", ascending=False)
print("\n\n===== OVERVIEW, sorterar på fallande =====")
print(summary_df.to_string(index=False))
