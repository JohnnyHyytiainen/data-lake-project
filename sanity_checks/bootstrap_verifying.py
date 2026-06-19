import duckdb

result = duckdb.sql("""
SELECT
    date_trunc('month', created_at) AS event_month,
    COUNT(*) AS pr_events
FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true)
WHERE event_type = 'PullRequestEvent' AND repo_name = 'apache/airflow'
GROUP BY event_month
ORDER BY event_month;
""").df()

print(result)
