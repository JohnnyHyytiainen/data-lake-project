import duckdb

# Jämförelsequery: hur många closed-events har repos med >= 5 matchade par?
# Broken ut per repo så vi kan se om pr_count ökat vs förväntan
result = duckdb.sql("""
    SELECT
        repo_name,
        pr_count,
        median_hours,
        p95_hours
    FROM read_parquet('./data/gold/pr_cycle_times/**/*.parquet', hive_partitioning=true)
    ORDER BY pr_count DESC
    LIMIT 20
""").df()

print(result)
