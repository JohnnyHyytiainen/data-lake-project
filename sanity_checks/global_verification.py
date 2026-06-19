import duckdb

# Global koll: finns FAKTISKA event_id-duplikater i Silver just nu?

result = duckdb.sql("""
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT event_id) AS distinct_event_ids,
    COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_rows
FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true);
""").df()

print(result)
