import duckdb

# Sanity check för att se om is_bot är värt att ha med i min intermediate DBT model eller inte.
# Förväntat är att jag får en tom DataFrame returnerad. Varför?
# Om denna ger NÅGRA rader: max(is_bot) i int_actor_behavior döljer sig en
# verklig inkonsekvens i datan, inte bara plockar ett konstant värde.
# Körs direkt mot Silver -- behöver inte vänta på staging-fixen.

is_bot_intermediate = duckdb.sql("""
SELECT
    actor_login,
    COUNT(DISTINCT is_bot) AS distinct_is_bot_values
FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true)
GROUP BY actor_login
HAVING COUNT(DISTINCT is_bot) > 1
""").df()

print(is_bot_intermediate)
