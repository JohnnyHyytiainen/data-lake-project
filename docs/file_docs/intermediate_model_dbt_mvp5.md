# Docs regarding what an intermediate model is in dbt and what use cases it has for me
*Written Monday 2026-06-22 by Johnny*

## Grain and granularity, 
`Grain` is the precise Kimball term for "what does a row in this table represent", the general definition, regardless of dimension. `Granularity` is often used synonymously, but strictly speaking more often refers to the level within a specific dimension - e.g. "daily granularity" vs. "monthly granularity" for time. 

---

## DBT as a whole
`ref()` builds the DAG automatically, everytime a model writes `{{ref('stg_githubevents')}}` instead of hardcoding a table name, dbt registers a "dependency arrow". If I  run `dbt run`, `dbt` sorts all `models` topologically based on those arrows, `staging` is always built *before* *intermediate*, *intermediate* always *before* a `mart` that references it. I never need to keep track of the order myself, `dbt` does it for you based on the `graph`.

`staging`/`intermediate`/`marts` is a convention, not a rule `dbt` enforces. `dbt` doesn't care what folders I have, it's `dbt Labs`' own *best-practices* guide that established the names, for the same reason `medallion architecture` is a convention: clear separation between "decoupled raw data" (staging), "reusable logic" (intermediate), and "business response" (marts). I built that separation quite instinctively in earlier sessions before I knew it had a name in the community.  

**Materialization choice** for `int_actor_behavior`: `view`, for the same reasons as `staging`. Its not a business case *yet*, it's a building block. Making it a `VIEW` (not TABLE) means it's always live against the latest `Silver data`, no extra rebuild discipline to keep track of, and I can still query it directly (`SELECT * FROM int_actor_behavior`) for `sanity checks` in my regular `sanity_checks/`-folder, unlike ephemeral materialization, where dbt bakes in the `SQL` as a `CTE` and I never get a `queryable object` to target `sanity checks` against.


---

## Bonus: What I should have in the portfolio if someone would ask.
I should have `dbt docs generate` + `dbt docs serve` to automatically generate a visual DAG of all my models and their `ref()`-dependencies. It'll cost 5 minutes of my time but gives a real and concrete picture over the entire data lineage chain.