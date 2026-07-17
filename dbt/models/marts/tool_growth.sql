/*
tool_growth.sql script
Mart modell: Räknar WatchEvents. Stars per repo och vecka, plus vilken
DE-tool kategori repot tillhör (tool_category)

Svarar på: Vilka DE tools i mitt dataset som växer snabbast.
Blir en riktig table och inte en VIEW som i stg_github_events.sql scriptet.
DuckDB sparar resultatet fysiskt, inte som en live fråga via VIEW.
 */
with
    source AS (
        SELECT
            *
        FROM
            {{ ref ('stg_github_events') }}
        WHERE
            event_type = 'WatchEvent'
    ),
    -- Klassificerar varje repo_name till EN tool_category (Första matchning vinner)
    -- Ordningen speglar config.py DE_KEYWORDS rakt av.
    -- pyspark måste testas FÖRE spark då "spark" är en substring av pyspark
    categorized AS (
        SELECT
            *,
            CASE
                WHEN repo_name ILIKE '%dbt%' THEN 'dbt'
                WHEN repo_name ILIKE '%airflow%' THEN 'airflow'
                WHEN repo_name ILIKE '%pyspark%' THEN 'pyspark'   -- FÖRE spark
                WHEN repo_name ILIKE '%spark%' THEN 'spark'
                WHEN repo_name ILIKE '%kafka%' THEN 'kafka'
                WHEN repo_name ILIKE '%flink%' THEN 'flink'
                WHEN repo_name ILIKE '%dagster%' THEN 'dagster'
                WHEN repo_name ILIKE '%prefect%' THEN 'prefect'
                WHEN repo_name ILIKE '%kestra%' THEN 'kestra'
                WHEN repo_name ILIKE '%apache-beam%' THEN 'apache-beam'
                WHEN repo_name ILIKE '%duckdb%' THEN 'duckdb'
                WHEN repo_name ILIKE '%delta-lake%' THEN 'delta-lake'
                WHEN repo_name ILIKE '%iceberg%' THEN 'iceberg'
                WHEN repo_name ILIKE '%apache-arrow%' THEN 'apache-arrow'
                WHEN repo_name ILIKE '%parquet%' THEN 'parquet'
                WHEN repo_name ILIKE '%avro%' THEN 'avro'
                WHEN repo_name ILIKE '%protobuf%' THEN 'protobuf'
                WHEN repo_name ILIKE '%trino%' THEN 'trino'
                WHEN repo_name ILIKE '%bigquery%' THEN 'bigquery'
                WHEN repo_name ILIKE '%airbyte%' THEN 'airbyte'
                WHEN repo_name ILIKE '%fivetran%' THEN 'fivetran'
                WHEN repo_name ILIKE '%dlt%' THEN 'dlt'
                WHEN repo_name ILIKE '%soda-core%' THEN 'soda-core'
                WHEN repo_name ILIKE '%data-contracts%' THEN 'data-contracts'
                WHEN repo_name ILIKE '%data-lineage%' THEN 'data-lineage'
                WHEN repo_name ILIKE '%polars%' THEN 'polars'
                WHEN repo_name ILIKE '%grafana%' THEN 'grafana'
                WHEN repo_name ILIKE '%data-engineering%' THEN 'data-engineering'  -- FÖRE data-engineer
                WHEN repo_name ILIKE '%data-engineer%' THEN 'data-engineer'
                WHEN repo_name ILIKE '%data-warehouse%' THEN 'data-warehouse'
                WHEN repo_name ILIKE '%data-lakehouse%' THEN 'data-lakehouse'
                ELSE 'unmatched'  -- Tripwire, ska ALDRIG få rader
            END AS tool_category
        FROM
            source
    ),

    weekly AS (
        SELECT
            date_trunc('week', created_at) AS week_start,
            repo_name,
            tool_category,
            COUNT(*) AS star_count
        FROM
            categorized
        GROUP BY
            1, 2, 3
    )
SELECT
    week_start,
    repo_name,
    tool_category,
    star_count,
    SUM(star_count) OVER (
        PARTITION BY repo_name
        ORDER BY week_start
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_stars
FROM
    weekly
ORDER BY
    week_start DESC,
    star_count DESC