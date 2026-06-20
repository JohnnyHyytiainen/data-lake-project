import duckdb

# Bot verifiering: finns min is_bot flagga i silver nu

is_bot = duckdb.sql("""
SELECT is_bot, COUNT(*) AS event_count,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true)
GROUP BY is_bot
""").df()

print(is_bot)


individual_accounts = duckdb.sql("""
SELECT DISTINCT actor_login, is_bot
FROM read_parquet ('data/silver/events/**/*.parquet', hive_partitioning = true)
WHERE actor_login IN (
    'github-actions[bot]', 'dependabot[bot]', 'flinkbot', 'gemabintang3108-prog', 'botAGI', 'bot-grabber', 'Lewatoto')
""").df()

print(individual_accounts)
