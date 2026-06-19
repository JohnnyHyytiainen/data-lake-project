# Kommentarer: Svenska
# Kod: Engelska
# Sanity check script för att se om refaktoriseringen av pr_action till event_action tog.
import duckdb


result = duckdb.sql("""
    DESCRIBE SELECT * 
FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true);
    
""").df()

# Förväntat: ingen pr_action längre utan -> event_action
print(result)
