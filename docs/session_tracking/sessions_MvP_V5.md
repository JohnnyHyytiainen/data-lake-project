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

- Complete mvp v5 - Update grafana dashboard to show new panels added earlier
    - *ongoing*



    

# TODO TECH DEBT SECTION:

- After schema change, look into my postgres DB acting odd after complete rebuild. Investigate logs showing:
    - *ongoing*

```
2026-06-19 13:25:31.888 UTC [27] LOG:  checkpoint starting: time
2026-06-19 13:25:33.109 UTC [27] LOG:  checkpoint complete: wrote 13 buffers (0.1%); 0 WAL file(s) added, 0 removed, 0 recycled; write=1.205 s, sync=0.010 s, total=1.221 s; sync files=10, longest=0.003 s, average=0.001 s; distance=108 kB, estimate=539 kB; lsn=0/6C1B238, redo lsn=0/6C09B78
2026-06-19 13:25:33.852 UTC [1226] FATAL:  role "airflow" does not exist
```



