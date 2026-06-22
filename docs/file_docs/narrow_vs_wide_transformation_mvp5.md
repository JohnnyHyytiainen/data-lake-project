# Distinction between Upstream and Downstream
*Written Monday 2026-06-22 by Johnny*

- Where does both my `bot`-analysis belong and what are those "things" actually called?

## is_bot in silver is on `row level`, a *narrow transformation*

- `is_bot` in silver is on **ROW LEVEL** and is called a **NARROW TRANSFORMATION**. My `_classify_is_bot()`-function in `bronze_to_silver.py` takes `actor_login` for *ONE* for and answers with either `True/False` for that specific row. It never needs to look at another row what so ever. `Spark` processes that row entirely isolated per partition, no data is being moved at all.

- This is the *definition* of a `narrow transformation` and basically what that means is, *one row in, one row out*. That is also *WHY* it is a *pure function*. (`Column` -> `Column`). The same input always gives the same output, no side effects and no shared state.

- This happens *upstreams* close to the source (Source in this aspect regards my source data, my `SSOT`)

---

## After `row level` and the *narrow transformation*
- My narrow transformation was the latest coding session. What I intend to do now with my `intermediate`-model in `dbt` functions on a completely different level. This works on **ENTITY LEVEL** and is called a **WIDE TRANSFORMATION**

- To answer the question:
    - "Does the `gemabintang3108-prog` user(actor) send unreasonable events in relation to number of repos?"

- I must compare *ALL* rows with the same `actor_login` against eachother. By using aggregating with statements such as `COUNT(*)`, `COUNT(DISTINCT event_type)`, `COUNT(DISTINCT repo_name)` **PER ACTOR**. This is a `GROUP BY` and a `GROUP BY` can *never* be calculated by row in isolation. 

- My engine(`DuckDB` in my case) must sort/move data such that *all* rows for the same actor gets put together first. `GROUP BY` is just `SQL`'s name for the same shuffle-forcing operation, the same as `Spark`'s `dropDuplicates()` requires a shuffle, for the data to be moved between partitions to be compared. 

I had knowledge about this prior to writing this from previous `bug` experiences and this is why I have to make this distinction. Two completely separate issues that belong in separate areas of my pipeline/my build.

---

## Two lenses, to lock it down.

**The Data Modeling lens:** 
- `is_bot` is a *degenerate dimension attribute* - a property stored directly on the fact row, cheap, ready at the time of writing. 

- `int_actor_behavior` on the other hand, builds something that behaves like a *derived dimension table* - one row per `actor_login`, an actor profile, created by aggregating the entire fact table. 

- The `marts/` layer joins against the *derived dimension* just like a `star schema fact` joins against a `real dimension`. The only difference here is that *this* dimension is built by the `database` itself, not loaded from a source system.

**The technical term for what im actually building:**
- `int_actor_behavior.sql` is `architecturally` the same pattern as a *feature store* (example, `Feast`, `Databricks Feature Store`, `Tecton`) in production ML: compute entity-level features *once*, store them as a shared layer, let multiple downstream models reuse the same computation. I have already implemented the `DRY` reasoning right for `is_bot`, this is the same principle, but "only" one notch higher.


| Level | Computational form | Technical term | Lives in |
|---|---|---|---|
| Row | Narrow transform, pure function | Row-level enrichment / degenerate dimension attribute | `bronze_to_silver.py` |
| Actor | Wide transform, `GROUP BY` | Entity-level aggregation / derived dimension / feature store pattern | `int_actor_behavior.sql` |
| Business query | `JOIN` of both | Business-facing mart | `bot_vs_human_activity.sql` |
