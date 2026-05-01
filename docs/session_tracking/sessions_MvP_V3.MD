# Session tracking notes for MvP v3
*Ongoing*

**Saturday 18/04-2026**
*Goals for today:*
- Setup `docker-compose.yml` file to be able to run `PySpark` in the same container as `producer`, `consumer` and `kafka`.
    - **Done**

- Write docs regarding `Spark` and `docker-compose`, `dbt` and `Airflow`
    - **Done**

**Sunday 19/04-2026**
*Goals for today:*
- Get all set up for scripts. That will include:
    - deps
    - `dbt_project.yml`
    - `profiles.yml`(DuckDB config)
    - `dbt debug` to verify that everything is able to talk to eachother.
        - **Done**

- Start to write my staging model. `stg_github_events.sql`, this will read my silver parquet files. This is the foundation that all my mart-models will be built upon.
    - **Done**

- Write my first mart `tool_growth.sql`
    - **Done**

- Write own docs/notes regarding `dbt` and `DuckDB` + `tool_growth.sql` script
    - **Done**


**Monday 20/04-2026**
*Goals for today:*
- Start writing on my `activity_heatmap.sql` and `pr_cycle_times.sql` dbt marts.
    - **Done**

- Write own docs/notes regarding `activity_heatmap.sql`, `pr_cycle_times.sql` and more details about `dbt`
    - *ongoing*

**Wednesday 22/04-2026**
*Goals for today:*
- Containerise DBT in Docker and test it to make sure everything runs smoothly.
    - **Done**

- Write more comprehensive docs regarding docker and dbt. More theory is needed.
    - *ongoing*


**Friday 01/05, Saturday 02/05, Sunday 03/05-2026**
*Goals for weekend:*
- Setup Airflow DAG
    - *ongoing*

- Containerize Airflow and update `docker-compose-yml`
    - *ongoing*

- Write Airflow and DAG docs
    - *ongoing*

- Start working on serving layer with `grafana`
    - *Ongoing* 

- Start planning on next steps for MVP v4 and beyond.
    - More questions that needs to be answered from the data.
    - Bots vs humans
    - Expand the data lake to maybe look for more data from github API + github archives

    - Incorporate a vector DB + embeddings to introduce a layer of RAG over my entire data lake
        - Retrieve or run queries on data for more insights with said models?
        