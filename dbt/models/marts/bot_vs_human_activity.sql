-- bot_vs_human_activity.sql mart
/*
Andel events per aktörskategori, per event-typ, per vecka.
Kategorierna håller MEDVETET isär två olika konfidensnivåer, 
ingen sammanslagning till en enda boolean.
---
'bot'                  
is_bot=true. [bot]-suffix är Githubs egna mekanism för registrerade Apps
-bot/_bot-suffix och dependabot/renovate/github-actions-prefix 
är en etablerad community convention, inte en GitHub-regel. Hög konfidens.

'suspected_automation'
is_suspected_behavioral_bot=true.
Volym+diversitet-heuristik mot en heavy-tailed
fördelning utan naturlig fall off(gräns för vad som ser ut som mänskligt vs bot)
Provisorisk tröskel (500), ingen labeled groundtruth bakom den. Omvärderas efter DE_KEYWORDS.

'human' - ingen av ovan.
'unknown' - actor_login utan matchning i int_actor_behavior.

Grain: en rad per (week_start, event_type, actor_category).
*/

{{ config(materialized='table') }}

WITH actor_classification AS (
    SELECT
        actor_login,
        is_bot,
        (unique_repos = 1
         AND unique_event_types = 1
         AND event_count > 500) AS is_suspected_behavioral_bot
    FROM
        {{ ref('int_actor_behavior') }}
),

events_with_category AS (
    SELECT
        e.event_type,
        date_trunc('week', e.created_at) AS week_start,
        CASE
            WHEN c.actor_login IS NULL THEN 'unknown'
            WHEN c.is_bot THEN 'bot'
            WHEN c.is_suspected_behavioral_bot THEN 'suspected_automation'
            ELSE 'human'
        END AS actor_category
    FROM
        {{ ref('stg_github_events')  }} e
    LEFT JOIN
        actor_classification c
        ON e.actor_login = c.actor_login
),

weekly_counts AS (
    SELECT
        week_start, event_type, actor_category,
        COUNT(*) AS event_count
    FROM
        events_with_category
    GROUP BY 
        week_start, event_type, actor_category
)

SELECT
    week_start, event_type, actor_category, event_count,
    ROUND(
        100.0 * event_count / SUM(event_count) OVER (PARTITION BY week_start, event_type),
        1
    ) AS pct_of_week_type
FROM
    weekly_counts
ORDER BY
    week_start, event_type, actor_category