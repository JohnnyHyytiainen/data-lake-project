-- stg_github_events.sql script
/* 
Min staging modell. Läser mina Silver parquet filer och exponerar dom som en SQL view i DUCKDB.
Ingen buisiness logik här utan bara ren, namngiven data.
Alla MART modeller refererar till den här viewn via ref('stg_github_events').
 */
-- read_parquet() med glob-mönster (**/*) hämtar alla .parquet-filer
-- rekursivt oavsett partitionsdjup.
-- hive_partitioning=true gör att DuckDB automatiskt läser year/month/day
-- ur mappstrukturen som extra kolumner -- Jag behöver inte parsa paths manuellt.
SELECT
    event_id,
    event_type,
    actor_login,
    repo_name,
    repo_id,
    cast(created_at AS timestamp) AS created_at,
    --- Payload fälten som PySpark redan extraherat ur JSON strängen
    -- commit_count och pr_number fick coalesce(0) redan i bronze_to_silver, så 
    -- dom är aldrig NULL här.
    commit_count,
    pr_number,
    pr_action, -- NULL för icke PR events. Förväntat beteende
    pr_merged, -- NULL för icke PR events
    cast(year as integer) AS year,
    cast(month as integer) AS month,
    cast(day as integer) AS day,
FROM
    read_parquet (
        '../data/silver/events/**/*.parquet',
        hive_partitioning = true
    )