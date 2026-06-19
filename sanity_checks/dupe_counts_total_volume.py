import duckdb


# con = duckdb.connect("data/dbt/github_lake.duckdb", read_only=True)

df = duckdb.sql("""
WITH dupe_counts AS (
    SELECT repo_name, pr_number, COUNT(*) AS open_events
    FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true)
    WHERE event_type = 'PullRequestEvent' AND event_action = 'opened'
    GROUP BY repo_name, pr_number
)
SELECT
    COUNT(*) AS total_distinct_prs,
    SUM(CASE WHEN open_events > 1 THEN 1 ELSE 0 END) AS prs_with_duplicates,
    SUM(open_events) AS total_opened_rows,
    SUM(open_events - 1) AS excess_rows
FROM dupe_counts;
    """).df()

# Förväntat: ingen pr_action längre utan -> event_action
print(df)
# con.close()
