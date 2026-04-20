-- pr_cycle_times.sql script
-- Mart modell: Beräknar PR-cykeltid opened -> closed per repo.
-- Svarar på: Hur lång tid en HEL PR cykel tar
-- Kräver en self-join eftersom öppnings / stängnings events är separata rader i samma tabell.
-- pr_number i Silver-schemat är det som gör att joinen är 1:1 istället för en cartesian product.

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
