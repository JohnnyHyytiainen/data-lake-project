--- int_actor_behavior.sql script
/*
Aggregerar github aktivitet per aktör(actor_login)
Underlag för beteendebaserad bot klassificering.

Grain: En rad per actor_login.
Källans grain (stg_github_events): En rad per event
Det här är en mellanmodell, ingen affärsfråga besvaras här, bara
återanvändbara mått som frmatida gold-modeller/gold-marts (t.ex bot_vs_human_activity.sql)
kan joina mot framöver
 */

{{ config(materialized='view') }}

SELECT
    actor_login,
    COUNT(*) AS event_count,
    -- Diversitet, inte bara volym. En aktör som rör 50 olika event typer
    -- och 100 olika repos beter sig fundamentalt annorlunda VS en som
    -- gör exakt samma sak om och om igen i ett enda repo, även om
    -- event_count är identiskt högt för båda.
    COUNT(DISTINCT event_type) AS unique_event_types,
    COUNT(DISTINCT repo_name) AS unique_repos,
    -- is_bot är funktionellt beroende av actor_login. Samma aktär ger
    -- ALLTID samma namnbaserade klassificering eftersom regeln bara tittar på
    -- login str. max() är en 'safe' aggregatfunktion för ett värde som redan är
    -- konstant per grupp.
    max(is_bot) AS is_bot
FROM
    {{ ref('stg_github_events') }}
GROUP BY
    actor_login