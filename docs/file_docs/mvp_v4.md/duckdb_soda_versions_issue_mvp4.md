# Issues regarding Duckdb versions AND sodacore.
When adding soda-core deps i stumbeled upon this:

```
 - duckdb==1.5.2

 + duckdb==1.0.0

```

This in of itself might *not* cause issues but that doesn't mean it *ISNT* an issue and it is important to understand this *before* i trigger more runs.

## The duckdb versions is a problem - But only for my local environment and not for my containers.
`uv add ....` solved my dependency-conflicts by picking that version that *both* packages accepts. `soda-core-duckdb` is depending on an older `DuckDB`-version, so the entire shared Python environment had to be downgraded to `1.0.0`. However, this doesn't affect the `Docker containers` one bit, each `container` builds its own isolated `Python environment` based on its `Dockerfile`, independent of my `pyproject.toml`. The `quality container` runs `DuckDB 1.0.0` and the `dbt container` runs its own version. They don't know about each other.

**The problem:** if I ever run `dbt` or `DuckDB` queries **locally outside** of `Docker` I am now running `1.0.0` instead of `1.5.2`. This may cause strange behavior if `github_lake.duckdb` was created with `1.5.2` **`(DuckDB has storage-format versioning)`**

**The solution:** Remove `soda-core-duckdb` from my local environment:
- ` uv remove soda-core-duckdb `

Verify that `1.5.2` is back:
- `uv pip show duckdb`

The fix didn't really take since it still shows:

```terminal
$ uv pip show duckdb
Name: duckdb
Version: 1.0.0
Location: C:***/***/***/**/*/*/
Requires:
Required-by: dbt-duckdb
```

`dbt-duckdb` allows Duckdb 1.0.0, this is a valid version according to its constraints. When UV solved the conflict with Soda it landed on 1.0.0 and locked it in uv.lock. Now that soda is removed uv isnt touching it. The lockfile is convervative and doesnt upgrade automatically. To solve this I will run this command:

- ` uv lock --upgrade-package duckdb && uv sync `

- **important note:** This wont affect my `airflow` runs, the containers build their own python environments from their `Dockerfiles`, they dont know about my *LOCAL* `uv.lock`.