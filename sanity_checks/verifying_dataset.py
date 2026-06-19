import duckdb


result = duckdb.sql("""
SELECT MIN(created_at) AS earliest, MAX(created_at) AS latest, COUNT(*) AS total
FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true);
""").df()

# Förväntat: ingen pr_action längre utan -> event_action
print(result)
