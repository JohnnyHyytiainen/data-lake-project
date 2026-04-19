# File for ease of use dbt commands worth knowing.

Navigate to the dbt folder via:  `cd dbt`
- `dbt run --select stg_github_events --profiles-dir .`
- A successful run should output somehting in the way of this:
```
Completed successfully
Created view model main.stg_github_events  [OK in 0.12s]
```
---

When that is successful its good practice to run a sanity check to confirm that `DuckDB` really can see my 1mil + silver records. This command opens an interactive `DuckDB` "shell" towards my DB file.

- `dbt show --select stg_github_events --profiles-dir . --limit 5`
- This is like running `.head(5)` with pandas df.
