# Session tracking notes for MvP v3.0.1+ - Tech debt cleanup
*Ongoing*
---

**Monday 18/05-2026**
*Goals for today:*

- Start working on clearing current tech-debt from previous MVP v1-v3.
    - Checkpoint file
        - **Done**
    - Update testing suite
        - *ongoing*
    - Documentation debt in form of serving.mmd flowcharts and write docs regarding checkpoint file fix
        - *ongoing*
--- 

**Monday 25/05-2026**
- Updated Airflow DAGs, fetched more data before demo tomorrow with R.E at Nex. Fetching more data from 2025.
    - **Done** 
    
**Tuesday 26/05-2025**
- PoC over data lake project shown
    - **Done**

--- 
**Monday 08/06-2026**
*Goals for today:*
- Update Github API token since current one is about to expire
    - **Done and working**
    - Example logs from `producer` in `docker`: `2026-06-08 17:45:10 | INFO | Cycle complete | sent=1 skipped_type=99, skipped_dupe=0`

---
**Tuesday 09/06-2026**

- DO NOT run `git clean -fdx`

---

**Friday 12/06-2026**

- Keep working on rebuilding test-suite for github actions ci pipeline. Only linting and formatting in current stage since porting over from pandas to PySpark.
    - **Done**

- Refactored `bronze_to_silver` to separate transformation logic from main function. Adhearing to SoC principles.
    - **Done**

---

**Friday 12/06-2026**
- Update previously destroyed `serving-layer JSON` with a `Grafana`-dashboard.
    - **Done**

- Write more extensive documentation regarding on how to setup serving layer once more if incidents happen again
    - **Done**

- Implement a flowchart/diagram with `.mmd`-code over `serving-layer`
    - *ongoing*

    