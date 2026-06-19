import duckdb


# con = duckdb.connect("data/dbt/github_lake.duckdb", read_only=True)

df = duckdb.sql("""
SELECT repo_name, pr_number, COUNT(*) AS open_events
FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true)
WHERE event_type = 'PullRequestEvent'
AND event_action = 'opened'
GROUP BY repo_name, pr_number
HAVING COUNT(*) > 1
ORDER BY open_events DESC
    """).df()

# Förväntat: ingen pr_action längre utan -> event_action
print(df)
# con.close()
