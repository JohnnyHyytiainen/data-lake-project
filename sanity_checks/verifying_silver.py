import duckdb


result = duckdb.sql("""
    SELECT COUNT(*) AS qualifying_close_events
FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true)
WHERE event_type = 'PullRequestEvent'
  AND (event_action = 'merged' OR (event_action = 'closed' AND pr_merged = True));
    
""").df()

# Förväntat: ingen pr_action längre utan -> event_action
print(result)
