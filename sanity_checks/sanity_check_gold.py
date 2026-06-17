import duckdb

result = duckdb.sql("""
    SELECT
        SUM(pr_count)    AS total_merged_prs,
        COUNT(*)         AS qualifying_repos,
        round(AVG(median_hours), 1) AS avg_median_hours
    FROM read_parquet('./data/gold/pr_cycle_times/**/*.parquet', hive_partitioning=true)
""").df()

print(result)
