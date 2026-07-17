# Session tracking notes for MVP v5 - Deeper analysis and wider insights
*Started since 2026-06-17*

---

**Wednesday 2026-06-17**
*Goals for today*

- Write sanity check scripts to confirm that issues with `pr_merge` is solved.
    - **Done**
    
- Start MVP v5 - Refactor `pr_cycle_times.sql`-mart
    - **Done**


---
**Wednesday 2026-06-17**
*Goals for today:*
- Start writing docs for schema renaming and schema change, 2 "phases", 9 steps
    **Done**


---

**Friday 2026-06-19**
*Goals for today:*


- Implement working schema change/schema update - Rename `pr_action` to `event_action` for future clarity.
    - First go through these scripts:
    - `bronze_to_silver.py` + `test_transforms.py` then validate it
        - **Done** 

- Validate transformation logic that the current schema change is working by running `pytest`-locally
    - **Done**

- Keep on working upwards with schema change
    - **Done**

---

**Saturday 2026-06-20**
*Goals for today:*

- Write docs regarding `is_bot` and how to approach this problem.
    - **Done**

- Branch out and start working on `is_bot` column to identify bot actors in my data
    - **Done**


---

**Monday 2026-06-22**
*Goals for today:*

*Intermediate model*
- Write docs regarding todays session and what is needed to implement an `intermediate/layer`, not in silver since that requires a `GROUP BY`-aggregation on the *ENTIRE* dataset instead of on each row.
    - **Done**

- Write sanity-check script to validate my logic and not get any unwanted issues downstream when building new marts. Script is `is_bot_sanity_intermediate_model.py`.
    - **Done**

- Write a new `dbt/models/intermediate/DBT_MODEL_NAME.sql` script to aggregate `event_count`, `unique_event_types`, `unique_repos` per `actor_login` from my staging model `stg_github_events`.
    - **Done**


*dbt mart*
- Start writing docs regarding upcoming `gold` `mart` to identify "suspicious bot behaviour"
    - **Done**
---

**Tuesday 2026-06-23**
*Goals for today:*
- Update daily todo's
    - **Done**



- Start writing dbt `bot_vs_human_activity.sql`-mart to include `human`, `bot` and `suspected_automation`
    **Done**

---

**Wednesday 2026-06-24**
*Goals for today:*

- Write docs regarding how to implement a wider scope of `DE_KEYWORDS`-search params
    - **Done**

- Expand list for `DE_KEYWORDS` to get more hits from relevant named repos on Github
    - **Done**


- Start to implement and write stand-alone script to test new `DE_KEYWORDS` with 24h `bootstrap`-data and analyze that data
    - **Done**


---

**Monday 2026-06-29**
*Goals for today:*

- Write new dbt mart docs regarding `repo_health.sql`-mart
    - **Semi-Done**, Still needs a bit of text to be completed.

    
- Start working on next step in mvp v5 which is `repo_health.sql`-mart
    - **Done**

---

**Wednesday 2026-07-01**
*Goals for today:*

- Complete sanity checks and look over `repo_health.sql`-mart to see if logic is valid
    - **Done**
    
---
**Thursday 2026-07-16**
*Goals for today:*

- Fix critical infra gaps (Postgres is skipping initialization in container)
    - **Done**

- Fill in data gap from days without collecting data
    - **Done**

- Clear checkpoint and silver layer of data and re-run entire pipeline flow
    - **Done**
    - `| INFO | Found 12201 total Bronze files | 0 already processed | 12201 new files to process`
    - `| INFO | Flattened 2927349 events to Silver schema | 499913 removed by filter + deduplication`
    - `| INFO | Wrote 2927349 Silver records -> /app/data/silver/events`
    - `"silver_row_count": 2927349`


---
**Friday 2026-07-17**
*Goals for today:*

- Update current grafana dashboard for new marts
    - *ongoing*




    

# TODO TECH DEBT SECTION:
