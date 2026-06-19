import duckdb

# -- Samma räkning, direkt mot Silver Parquet (samma mönster som dina tidigare sanity-checks)

result = duckdb.sql("""
SELECT COUNT(*) AS via_direct_parquet
FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true)
WHERE event_type = 'PullRequestEvent'
  AND event_action = 'opened'
  AND repo_name = 'apache/airflow';
    
""").df()

# Förväntat: ingen pr_action längre utan -> event_action
print(result)
