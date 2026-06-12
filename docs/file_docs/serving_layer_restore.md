## Rebuilding the serving layer
After *nuking* my `serving layer` by accident, it is now time to redo it and update my docs to make it easier if accidents were to happen once again. Learn by doing and repetition is the father of learning etc etc...

---

### Steps to rebuild everything.
- Download correct plugin version by writing while in root:
    - `curl -LO https://github.com/motherduckdb/grafana-duckdb-datasource/releases/download/v0.4.0/motherduck-duckdb-datasource-0.4.0.zip`

1) - Create a new `plugins`-folder.
    - `mkdir -p serving/grafana/plugins`

2) - Extract directly to correct spot
    - `unzip motherduck-duckdb-datasource-0.4.0.zip -d serving/grafana/plugins/motherduck-duckdb-datasource`

3) - Verify that you downloaded correct files:
    - `ls serving/grafana/plugins/motherduck-duckdb-datasource/`
        - You should see something similar to: ` gpx_motherduck-duckdb-datasource_linux_amd64` (binary), `plugin.json`, etc etc.

4) - Confirm that the `dashboard`-folder is empty:
    - `ls serving/grafana/dashboards/`
        - You should see *nothing*

5) - Verify that my gold folders exist:
    - `ls data/gold/`
        - You should see: `activity_heatmap/  pr_cycle_times/  tool_growth/`

---

### Once above steps are confirmed its time to start and verify.

1) - Start only the grafana-service(the rest should already be on)
    - `docker compose up -d grafana`

2) - Just to be sure, restart the container.
    - `docker-compose restart grafana`

3) - Doublecheck that the bind mount actually works within the container with:
    - `docker exec grafana ls //var//lib//grafana//plugins//motherduck-duckdb-datasource//` (double // if using bash terminal)
        - You should see something similar to:
        ```
        CHANGELOG.md
        LICENSE
        README.md
        go_plugin_build_manifest
        gpx_duckdb_datasource_darwin_amd64
        gpx_duckdb_datasource_darwin_arm64
        gpx_duckdb_datasource_linux_amd64
        gpx_duckdb_datasource_linux_arm64
        gpx_duckdb_datasource_windows_amd64.exe
        img
        module.js
        module.js.LICENSE.txt
        module.js.map
        plugin.json
        ```

- Once this is working you can move on to the next step.

### Finding your Datasources
1) - Navigate to your grafana.
    - It should be: `localhost:3000`

2) - Enter your supersecret username and password

3) - Left menu, click it and look for Connections and click it.
    - Click on `Data Sources`
    - Click on your `DuckDB - GitHub Lake`-data source
    - Click `Test` - If your Data source is working. It is now time to explore it and start to build your Dashboard JSON.

--- 

### Exploring and building your Dashboard.
- Before building the dashboard its always good practice to map your gold tables(takes a few minutes)

1) - Navigate to `explore data` button on the top right side.

2) - Run a few queries, I recommend these: (press `Code`)
    - `SHOW TABLES;`
    - `DESCRIBE tool_growth;`
    - `DESCRIBE activity_heatmap;`
    - `DESCRIBE pr_cycle_times;`

3) - Start building your graphs and do EDA on the golden layer as you choose.

## Current de_community.json for grafana dashboard is completed BUT
- Since previous EDA in this data I notice something off with my current JSON. The `heatmap` shows one of the most active days as SUNDAY. Which is incorrect from previous EDA. Therefore I must verify my previous insights using this query. Run this query directly in `Grafana Explore` against my OLAP duckdb:

```sql
SELECT 
    day_of_week,
    SUM(event_count) AS total_events,
    ROUND(100.0 * SUM(event_count) / SUM(SUM(event_count)) OVER (), 1) AS pct
FROM activity_heatmap
GROUP BY day_of_week
ORDER BY day_of_week;
```
- This query above gives `sql: Scan error on column index 1, name "total_events": unsupported Scan, storing driver.Value type *big.Int into type *string: Could not process SQL results`-error. The reason for this is that `DuckDB` is "friendlier" to the user with its datatypes, friendlier than the most database engines. `BIGINT` is 64-bit , BUT `SUM()` of a `BIGINT`-column can in *theory* swarm over and become bigger than 64-bit, therefore `duckdb` promotes the result to a `HUGEINT` which is 128-bit. The `Grafana`-plugin expects `int64` and NOT `big.Int`, therefore it crashes. 

- The correct query and fix is to explicitly use `CAST` to FORCE it back into a `BIGINT`
```sql
SELECT 
    day_of_week,
    CAST(SUM(event_count) AS BIGINT) AS total_events,
    ROUND(100.0 * CAST(SUM(event_count) AS BIGINT) 
               / CAST(SUM(SUM(event_count)) OVER () AS BIGINT), 1) AS pct
FROM activity_heatmap
GROUP BY day_of_week
ORDER BY day_of_week;
```

This query gives me the proper results, and as a sidenote: I will stumble upon this issue again when working with `Grafana`, therefore a good rule of thumb is to always use `CAST` to a `BIGINT` when i see `big.Int` in `Grafana`.

Results from query:
| day_of_week | total_events | pct |
|:---|:---|:---|
|1 | 326340 | 16 |
|2 | 299741 | 14.7 |
|3 | 297503 | 14.6 |
|4 | 283844 | 13.9 |
|5 | 289763 | 14.2 |
|6 | 247193 | 12.1 |
|7 | 293629 | 14.4 |

---

### Doublechecking my mart model:
- Doublecheck that my logic was sound when i wrote my `activity_heatmap.sql`-mart
    - `cat dbt/models/marts/activity_heatmap.sql`

- The result:

```sql
activity_heatmap.sql script
Mart modell: Räknar events per timme och veckodag
Svarar på: När DE communityt är aktivt på Github där resultatet är en 7x24 matris,
(7 veckodagar x 24 timmar) som Grafana kan visualisera som en HEATMAP
 */
WITH
    source AS (
        SELECT
            *
        FROM
           {{ ref ('stg_github_events') }}
    ),
    extracted AS (
        SELECT
            extract(
                'isodow'
                FROM
                    created_at
            ) AS day_of_week,
            extract(
                'hour'
                FROM
                    created_at
            ) AS hour_of_day,
            event_type
        FROM
            source
    )
SELECT
    day_of_week,
    hour_of_day,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (), 4) AS activity_share
FROM
    extracted
GROUP BY
    1,
    2
ORDER BY
    day_of_week,
    hour_of_day
```