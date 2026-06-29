-- repo_health.sql script
-- Mart-modell: ett sammansatt "aktivitets hälsomått" per repo, baserat på
-- mänsklig (is_bot = false) PR-cykeltid, commit volym och star tillväxt.
-- Version 1; enbart mänsklig aktivitet, INNER JOIN-population (krävs i alla tre komponenter)
-- percentile rank normalisering, lika vikt (1/3 var krävs).

with
    pr_events as (
        select
            repo_name,
            pr_number,
            event_action,
            pr_merged,
            created_at,
            is_bot
        from {{ ref('stg_github_events') }}
        where
            event_type = 'PullRequestEvent'
            and pr_number > 0
    ),

    pr_opened as (
        select repo_name, pr_number, created_at as opened_at
        from pr_events
        where
            event_action = 'opened'
            and is_bot = false  -- bara mänskligt öppnade PRs räknas som "human made work"
    ),

    pr_closed AS (
        SELECT repo_name, pr_number, MIN(created_at) AS closed_at
        FROM pr_events
        WHERE
            event_action = 'merged'
            OR (event_action = 'closed' AND pr_merged = true)
        GROUP BY repo_name, pr_number
    ),

    pr_component AS (
        SELECT
            o.repo_name,
            COUNT(*) AS pr_count,
            ROUND(median(datediff('second', o.opened_at, c.closed_at) / 3600.0), 1) AS median_pr_hours
        FROM pr_opened o
        INNER JOIN pr_closed c
            ON o.repo_name = c.repo_name AND o.pr_number = c.pr_number
        WHERE c.closed_at > o.opened_at
        GROUP BY o.repo_name
        HAVING COUNT(*) >= 5  -- samma tröskel/motivering som pr_cycle_times.sql
    ),

    commit_component AS (
        SELECT
            repo_name,
            SUM(commit_count) AS total_commits
        FROM {{ ref('stg_github_events') }}
        WHERE
            event_type = 'PushEvent'
            AND is_bot = false
        GROUP BY repo_name
    ),

    star_component AS (
        SELECT
            repo_name,
            COUNT(*) AS total_stars
        FROM {{ ref('stg_github_events') }}
        WHERE
            event_type = 'WatchEvent'
            AND is_bot = false
        GROUP BY repo_name
    ),

    qualifying AS (
        SELECT
            pr.repo_name,
            pr.pr_count,
            pr.median_pr_hours,
            cm.total_commits,
            st.total_stars
        FROM pr_component pr
        INNER JOIN commit_component cm ON pr.repo_name = cm.repo_name
        INNER JOIN star_component st ON pr.repo_name = st.repo_name
    ),

    ranked AS (
        SELECT
            repo_name,
            pr_count,
            median_pr_hours,
            total_commits,
            total_stars,
            -- Lägre cykeltid = friskare -> vänd percentilen (1 - rank)
            ROUND(1 - percent_rank() OVER (ORDER BY median_pr_hours), 3) AS pr_speed_score,
            -- Högre commit-volym = friskare -> rak percentil
            ROUND(percent_rank() OVER (ORDER BY total_commits), 3) AS commit_volume_score,
            -- Högre stjärn-tillväxt = friskare -> rak percentil
            ROUND(percent_rank() OVER (ORDER BY total_stars), 3) AS star_growth_score
        FROM qualifying
    )

SELECT
    repo_name,
    pr_count,
    median_pr_hours,
    total_commits,
    total_stars,
    pr_speed_score,
    commit_volume_score,
    star_growth_score,
    ROUND((pr_speed_score + commit_volume_score + star_growth_score) / 3.0, 3) AS repo_health_score
FROM ranked
ORDER BY repo_health_score DESC