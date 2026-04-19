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


- Write own docs/notes regarding `dbt` and `DuckDB`
    - *ONGOING*