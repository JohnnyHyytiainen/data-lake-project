# Own docs for activity_heatmap.sql script


## Vad scriptet gör och syftet.
Syftet med scriptet är:   
Räkna antal events per timme och veckodag för att kunna svara på NÄR DE communityt är aktivt på Github. Resultatet av `activity_heatmap.sql` kommer bli en 7x24 matris(7 veckodagar x 24 timmar) som Grafana kan visualisera som en HEATMAP.  
I detta script ligger intresset att ta reda på ALLA event typer. Jag vill se aktivitetsmönstret i sin helhet och inte bara stars eller PullRequests


## Hur fungerar scriptet i sin helhet?
Alla event-typer är återigen intressanta här, Jag vill se aktivitetsmönstret i sin helhet, inte bara stars eller PRs.
`extract()` plockar ut en specifik del ur en timestamp.
`isodow = ISO` day of week: 1=måndag, 7=söndag, det är och ska vara mer förutsägbart än dow som varierar mellan databaser. Normaliserat värde 0-1 för att göra heatmap färgsättningen enklare i Grafana. `round()` med 4 decimaler ger lagom precision.

```sql
WITH source AS (
    SELECT *
    FROM {{ ref('stg_github_events') }}
),
extracted AS (
    SELECT
        extract('isodow' FROM created_at) AS day_of_week,
        extract('hour' FROM created_at) AS hour_of_day,
        event_type
    FROM source
)
SELECT
    day_of_week,
    hour_of_day,
    COUNT(*) AS event_count,
    ROUND(
        COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (),
        4
    ) AS activity_share
FROM extracted
GROUP BY 1, 2
ORDER BY day_of_week, hour_of_day
```

Värt att notera: `over ()` utan någon `PARTITION BY`, det är ett fönster som är över hela resultat tabellen. Det innebär att `sum(count(*)) over ()` är det **totala** antalet events i hela datasetet, och `activity_share` berättar vad varje timme och dag kombination utgör av den totalen. Det är samma `window function` som i `tool_growth.sql`, men med ett ännu bredare fönster.