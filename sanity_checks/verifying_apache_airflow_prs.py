import duckdb

# Samma koll som global_verification.py, avgränsad till apache/airflow PR-events

result = duckdb.sql("""
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT event_id) AS distinct_event_ids,
    COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_rows
FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true)
WHERE repo_name = 'apache/airflow' AND event_type = 'PullRequestEvent';
""").df()

print(result)
