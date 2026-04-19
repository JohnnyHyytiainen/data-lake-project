# Own docs for dbt, DuckDB + dbt and its purpose.
Egna anteckningar och dokumentation av insikter och kunskap kring `dbt` och styrkan med `DuckDB`.


## DBT eller data build tool.
dbt är som ett "recept system" för SQL omvandlingar(CTE, Common Table Expression/s).

- Styrkan med `dbt` jämfört med `PySpark` är att dbt hanterar beroenden automatiskt. Jag skriver t.ex `ref('stg_github_events')` i `tool_growth.sql` och dbt förstår automatiskt att staging modellen måste köras INNAN mart modellen. Jag slipper alltså hålla koll ordningen då exekveringsordningen(Ordningen allting körs i) hanteras automatiskt.

## DuckDB som target. Varför?
`DuckDB` är som `SQLite` fast byggt för analytiska queries, dvs OLAP. Den läser mina gold .parquet filer direkt från disk utan att jag ens behöver en DB-server öht. För min MVP och som eget standalone projekt duger det utmärkt. `dbt` står för transformations logiken och `DuckDB` används som motorn i det hela.