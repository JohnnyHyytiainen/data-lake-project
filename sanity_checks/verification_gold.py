# Kommentarer: Svenska
# Kod: Engelska
# verifierings script för att se om refaktoriseringen av pr_action till event_action tog.
import duckdb


con = duckdb.connect("data/dbt/github_lake.duckdb", read_only=True)

df = con.sql("""
SELECT repo_name, pr_count
FROM pr_cycle_times
WHERE repo_name IN (
    'apache/airflow', 'dagster-io/dagster', 'duckdb/duckdb',
    'pola-rs/polars', 'PrefectHQ/prefect', 'trinodb/trino'
)
ORDER BY pr_count DESC;
    """)

# Förväntat: ingen pr_action längre utan -> event_action
print(df)
con.close()
