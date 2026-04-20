# Own docs for pr_cycle_times.sql script


## Vad scriptet gör och syftet.
Syftet med scriptet är:  
Att svara på frågan "för varje PR som öppnades, finns det ett matchande event där samma PR stängdes?" och för att beräkna en PullRequests cykels tid. Från att en PR öppnas till att den stängs. PER REPO.  
Scriptet kommer kräva en self join eftersom att öppnings och stängsnings eventen är separata rader i samma tabell. `pr_number` i mitt Silver Schema är det som möjliggör att joinen blir 1:1 istället för en Cartesian product(Som jag lärde mig den jobbiga vägen..)

---
## Själva SQL scriptet och teori bakom det:

Jag filtrera ut enbart `PullRequestEvents` ur staging. Jag behöver bara de kolumner som är relevanta för cykeltids beräkningen. `pr_number = 0` är `coalesce`-defaultvärdet från `bronze_to_silver` för events som saknar PR data filtrerar jag bort. 

Vänster sida av `self-joinen`: ALLA PR öppningar. Varje rad representerar starten av en PR-cykel.  
Höger sida av `self-joinen`: ALLA PR stängningar. Jag inkluderar bara merged PRs för att mäta "lyckade" cykler. `closed without merge` = övergiven PR, inte är inte meningsfullt för mig att mäta. 

`Self-joinen`: parar ihop varje öppning med sin matchande stängning. `INNER JOIN` innebär att jag bara behåller PRs där BÅDA events finns i datasetet, om jag saknar antingen öppning eller stängning (t.ex för PRs öppnade före nov 2025) faller raden bort. 
join-nyckeln `repo_name` + `pr_number` garanterar en 1:1 matchning. `datediff` i timmar ger mig cykeltiden som ett enkelt heltal. `epoch_seconds` är DuckDB "språk" för att räkna sekunder mellan två timestamps, som sedan konverteras om till timmar. Jag vill filtrera bort negativa cykeltider, det händer om stängning loggas före öppning pga klockskillnader i raw data. Aggregera per repo: median och p95 cykeltid. `percentile_cont` är en WINDOW FUNCTION som räknar fram exakt procent värde, det är mycket mer exakt än att räkna rader om jag förstått rätt.

```sql
with
    pr_events AS (
        SELECT
            repo_name,
            pr_number,
            pr_action,
            pr_merged,
            created_at
        FROM
           {{ ref ('stg_github_events') }}
        WHERE
            event_type = 'PullRequestEvent'
            AND pr_number > 0
    ),
    opened AS (
        SELECT
            repo_name,
            pr_number,
            created_at AS opened_at
        FROM
            pr_events
        WHERE
            pr_action = 'opened'
    ),
    closed AS (
        SELECT
            repo_name,
            pr_number,
            created_at AS closed_at
        FROM
            pr_events
        WHERE
            pr_action = 'merged'
    ),
    joined AS (
        SELECT
            o.repo_name,
            o.pr_number,
            o.opened_at,
            c.closed_at,
            round(
                datediff('second', o.opened_at, c.closed_at) / 3600.0,
                1
            ) AS cycle_hours
        FROM
            opened o
            INNER JOIN closed c ON o.repo_name = c.repo_name
            AND o.pr_number = c.pr_number
        WHERE
            c.closed_at > o.opened_at
    )
SELECT
    repo_name,
    count(*) AS pr_count,
    round(median(cycle_hours), 1) AS median_hours,
    round(quantile_cont(cycle_hours, 0.95), 1) AS p95_hours
FROM joined
GROUP BY repo_name
HAVING count(*) >= 5
ORDER BY pr_count DESC
```