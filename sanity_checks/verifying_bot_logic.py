import duckdb

# Bot verifiering om det FINNS users med bot i början eller om det bara är dum paranoia.

bot_verification = duckdb.sql("""
SELECT DISTINCT actor_login
FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true)
WHERE lower(actor_login) LIKE 'bot-%' OR lower(actor_login) LIKE 'bot_%'
""").df()

print(bot_verification)
