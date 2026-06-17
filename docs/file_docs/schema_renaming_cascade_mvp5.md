# Docs regarding schema renaming and the intricate details surrounding it.
Renaming a `schema` isnt as "easy" as it sounds. It can and probably *will* have major effects downstream in the pipeline.

- What depends on what in this case? Well...

`bronze_to_silver.py` creates the `column` --> `.parquet`-files from my silver-layer is affected, i need to purge + recreate them. A new cold run is needed. Which will also affect my `silver_checks.yml` script with `soda core` AND at the same time affect my `stg_github_events.sql`-dbt model, which in itself forces me to re-create my `pr_cycle_times.sql`-mart, `tool_growth.sql`-mart and lastly my `activity_heatmap.sql`-mart.


--- 

## To get the entire picture I can(and WILL) break it down into 3 phases.

* **Phase 1:** The changes in my code. The files I must update in one 'swoop' is:
1) `bronze_to_silver.py` - Here I *must* change `alias("pr_action")` to `alias("event_action")` in my `_transform()`-function. This will be the only change in my source.

2) `silver_checks.yml` - Here i must update EVERYWHERE where `pr_action` is referenced in my `Soda Core`-data contracts.

3) `stg_github_events.sql` - Here I must *verify* if I have anything with my old column name `pr_action`.

4) `pr_cycle_times.sql` - **EVERY** `WHERE pr_action = ..` + `AND pr_action = ..` *MUST* be changed to `event_action`. This is the model that is the *most* sensitive to change.

5) `tool_growth.sql` & `activity_heatmap.sql` - These marts/models *MUST* be verified just to make *sure* that these dont reference that Column directly.

* **Phase 2:** The `cold run` 
6) Here i *MUST* purge my `silver`-layer data *AND* purge my `checkpoint`-file. All existing `.parquet` files with the old column name will be lost and cleared.

7) Run `bronze_to_silver.py` against the entire Bronze history. Silver is recreated from scratch with `event_action` as the column name.

* **Phase 3:** Build upstreams.
8) `Soda quality checks` are ran, either through `Airflow DAGs` or manually. Here I will Validate that `event_action` exists and the Data contracts are held.

9) `docker compose build dbt` + `docker compose run --rm dbt` - Golden layer is recreated from the new Silver layers data.