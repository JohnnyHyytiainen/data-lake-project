# Docs regarding repo_health dbt mart
*written 2026-06-29*

- To better understand the logic and reasoning behind the `repo_health`-mart I can divide it in to different sections, with the first being:

The PR component - the `is_bot`-place

This is the most underrated(IMO) design question in all of mart, so I should tackle it first. 

* The question is: where should `is_bot = false` be filtered in.
    - on the opened line, the closed line, or both?


If I were to think of a `PR-cycle` as a relay race with two participants: the one who opens (writes the code, does the work) and the one who closes/merges (clicks the button, often a completely different role). They really answer to completely different questions..

The `actor_login` of the `opened` line = who did the actual development work. This is the "was the work human?" question.

The `actor_login` of the `closed` line = who happened to press merge. Could be a human, could be `github-actions[bot]` via the Merge Queue, completely independent of who wrote the code.


If I were to filter on `is_bot = false` on both sides, I would risk losing perfectly legitimate human `PRs` just because they were merged via an automated Merge Queue, exactly the kind of Merge Queue mechanism the `pr_cycle_times.sql`-mart already documents. That would be punishing *human work* for an irrelevant detail in how it was closed.

* The decision should then be to: filter `is_bot = false` *only* on the **opened side**. The closed side remains unfiltered, since who happened to merge it doesn't matter if all the work was done by a human.

---

The second section regards to the `Commit and Star components`. This is pretty straight forward and there shouldn't be a any kind of two sided nuance here. `actor_login` on a `PushEvent/WatchEvent` line is unambiguously "who did this action". Straight forward filter and no discussion needed.

---

The third section regards to the population and why an `INNER JOIN` actually solves more than one problem in my instance.

- Problem 1: `NULLs` If a repo is missing a component (example, 0 WatchEvents), a `LEFT JOIN` would give `total_stars = NULL` for that repo. Then when I calculate `(pr_speed_score + commit_volume_score + star_growth_score) / 3.0`. `NULLs` will spread throughout the addition. `5 + 3 + NULL = NULL`. The result would be that `repo_health_score` becomes a silent `NULL` for that repo, no crash, no error message, no nothing and just a repo invisibly dropping out of the `ORDER BY` result. Exactly the kind of silent data loss im already *allergic* to in this project (the `PyArrow` schema collapse I had before is the same pattern: *no crash, just data silently disappearing*)

- Problem 2: A fair basis for comparison. `percent_rank()` answers "where do I rank compared to the others?", but that requirement assumes that everyone in the comparison is *actually* measured on the *same* things. Ranking a repo with all three signals against one with just one signal is like comparing a triathlete to someone who only rode a bike, the numbers are not comparable even if they happen to be in the same table.

- Problem 3: `pr_component`s `having count(*) >= 5` already filters out low-active repos *before* `qualifying` is even built. This means that I don't have to invent separate min-thresholds for `total_commits/total_stars`, they inherit the "protection" for free via the PR requirement. *One threshold and not three*.

- "Problem" 4: Why this **ISN'T** the "same" `cartesian product` trap I accidently coded before when porting over from `Pandas` to `PySpark`. `pr_number` was added to the Silver schema precisely because a `JOIN` on just `repo_name` between row-level data `(M rows opened × N rows closed)` created a `Cartesian product`. Here the situation is different: `pr_component`, `commit_component`, and `star_component` already have `GROUP BY repo_name`, each delivering exactly one row per `repo_name` before the join even occurs. A join between three already aggregated `one-row-per-key` tables on the same key can **never** fan out, regardless of whether I join on just `repo_name`. The grain, not the join condition itself, is what protects me here.

---

