/*
activity_heatmap.sql script
Mart modell: Räknar events per timme och veckodag
Svarar på: När DE communityt är aktivt på Github där resultatet är en 7x24 matris,
(7 veckodagar x 24 timmar) som Grafana kan visualisera som en HEATMAP
 */
WITH
    source AS (
        SELECT
            *
        FROM
           {{ ref ('stg_github_events') }}
    ),
    extracted AS (
        SELECT
            extract(
                'isodow'
                FROM
                    created_at
            ) AS day_of_week,
            extract(
                'hour'
                FROM
                    created_at
            ) AS hour_of_day,
            event_type
        FROM
            source
    )
SELECT
    day_of_week,
    hour_of_day,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (), 4) AS activity_share
FROM
    extracted
GROUP BY
    1,
    2
ORDER BY
    day_of_week,
    hour_of_day