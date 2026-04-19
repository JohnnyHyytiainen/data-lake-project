/*
tool_growth.sql script
Mart modell: Räknar WatchEvents. Stars per repo och vecka.
Svarar på: Vilka DE tools i mitt dataset som växer snabbast.
Blir en riktig table och inte en VIEW som i stg_github_events.sql scriptet.
DuckDB sparar resultatet fysiskt, inte som en live fråga via VIEW.
 */
with
    source AS (
        SELECT
            *
        FROM
            {{ref ('stg_github_events')}}
        WHERE
            event_type = 'WatchEvent'
    ),
    weekly AS (
        SELECT
            date_trunc ('week', created_at) AS week_start,
            repo_name,
            COUNT(*) AS star_count
        FROM
            source
        GROUP BY
            1,
            2
    )
SELECT
    week_start,
    repo_name,
    star_count,
    SUM(star_count) OVER (
        PARTITION BY
            repo_name
        ORDER BY
            week_start ROWS BETWEEN UNBOUNDED PRECEDING
            AND current ROW
    ) AS cumulative_stars
FROM
    weekly
ORDER BY
    week_start DESC,
    star_count DESC