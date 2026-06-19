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

    

- After schema change, look into my postgres DB acting odd after complete rebuild. Investigate logs showing:
    - **ongoing** 

```
2026-06-19 13:25:31.888 UTC [27] LOG:  checkpoint starting: time
2026-06-19 13:25:33.109 UTC [27] LOG:  checkpoint complete: wrote 13 buffers (0.1%); 0 WAL file(s) added, 0 removed, 0 recycled; write=1.205 s, sync=0.010 s, total=1.221 s; sync files=10, longest=0.003 s, average=0.001 s; distance=108 kB, estimate=539 kB; lsn=0/6C1B238, redo lsn=0/6C09B78
2026-06-19 13:25:33.852 UTC [1226] FATAL:  role "airflow" does not exist
```



