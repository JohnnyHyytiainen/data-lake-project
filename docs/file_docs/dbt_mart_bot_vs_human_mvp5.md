# Docs regarding dbt model/mart. More specifically - `bot_vs_human_activity.sql` mart.
Previously in MVP 5 I added a column `is_bot` with a `True/False` `boolean`. That was intended to work as a "soft" filter for `bot`-suffixes. Actor names ending with this:
```python
    return (
        login.endswith("[bot]")
        | login.endswith("-bot")
        | login.endswith("_bot")
        | login.endswith("dependabot")
        | login.endswith("renovate")
        | login.endswith("github-actions")
    )
```
With these suffixes and *KNOWN* bot actors it is 'easy' to flag them as bots. Biggest reason why is because `Github` automatically(?) adds the `[bot]` or bot accounts with `[]`. A normal person *cant* create an account with square brackets in its username, plus accounts such as `dependabot`, `renovate` or `github-actions` are 100% an automated process.

---

## How do I identify other actors that ARENT human?
The business rule, where do I draw the line between "suspicious" and "normal" when coming to github account behaviours? This decision belongs in `bot_vs_human_activity.sql` because this is a business related question. If for instance a future model/mart want the raw messurements without my specific thresholds, for example a `top_contributors.sql`-mart that mart should not inherit my `bot-decision` automatically.


---

### A new pattern - A mart that join staging AND intermediate.
This will be the first time a `mart` gets data from TWO separate layers at the same time:
- `stg_github_events` **Grain:** `event`, gives me `event_type` and `created_at`
- `int_actor_behavior` **Grain:** `actor`, gives me the classification metrics.

This is the same structure as when a `fact`-table is `joined` to a `dimension`-table in a `star schema`, only here the `dimension` is derived from the database instead of loaded from a source system.

- Why is this `join` safe compared to the *fan-out* bug I had in `pr_cycle_times`?
    - `int_actor_behavior` has exactly one row per `actor_login` per construct. This is what `GROUP BY actor_login` guarantees. A `JOIN` against a table with a guaranteed `UNIQUE KEY` can never multiply rows on that side. There is *NO* `fan-out`-risk no matter how many `events` each actor has.

---

## Window function for shares of the total
To make this work with identifying 'suspicious' behaviour I will use `window functions`.
`GROUP BY` collapses rows, that means I lose the detail to get a total. But for this I will want **both** : 

- *One* row per `(week, event type, category)` 
- *and* to know what percentage of the total events for that type are for that week. 

A single `GROUP BY`-statement alone can't give me that, because the total depends on the same grouping I've already collapsed away..

The solution is a `window function`: `SUM(event_count) OVER (PARTITION BY week_start, event_type)`. Think of it like "a scoreboard",  `GROUP BY` per team gives me one row per team, I lose each players row. A `window function` leaves each players row intact, but "whispers" the teams total to each player anyway. No collapse, just an extra column calculated over a defined group of related rows.

---
