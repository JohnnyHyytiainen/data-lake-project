# Docs for stg_github_events.sql script
Egna notes kring `stg_github_events.sql` scriptet.

## Vad det här scriptet är och innebär?
stg_github_events.sql

Staging-modell som läser Silver Parquet-filer och exponerar dem som en SQL VIEW i DuckDB. Ingen business-logik här bara ren, namngiven data. Alla mart-modeller refererar till den här VIEW'n via `ref('stg_github_events')`. `read_parquet()` med glob-mönster (**/*) hämtar alla .parquet filer rekursivt oavsett partitionsdjup. 

`hive_partitioning=true` gör att DuckDB automatiskt läser year/month/day ur mappstrukturen som extra kolumner. Jag behöver inte parsa paths manuellt. `created_at` kommer från Silver som en sträng och castar till `TIMESTAMP` så att mart modellerna kan göra `date_trunc()`, `extract()` etc direkt.

payload är en JSON string som `PySpark` redan har extraherat ur JSON strängen. `commit_count` och `pr_number` fick `coalesce(0)` i `bronze_to_silver.py` scriptet så dom är aldrig `NULL` här. `stg_github_events.sql` är en 1:1 mappning mot mitt silver schema med rätt typer för mart modellerna.

```sql
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
```