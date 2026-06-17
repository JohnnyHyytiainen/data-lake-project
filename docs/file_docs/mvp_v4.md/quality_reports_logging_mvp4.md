# Own docs regarding quality reports logging with Soda core circuit breaker
Docs regarding the importance of `quality_reports`-file and theory behind *why* it is important.

## What is quality reports and why is it needed?
The concept im referring to here is called `Data Observability`.

- So what *is* `Data Observability?`

> Data observability is the difference between *knowing* that the pipeline ran and *understanding* what it found when it ran. `Soda Core` gives me a circuit breaker, it stops bad data from reaching Gold. But without logging, its like a smoke alarm without a logbook: I know the alarm went off, but not exactly when, how often, or if it's getting more frequent over time.

- A good similarity is this:
    - The difference between an engineer and a good engineer is the steps the good engineer takes when it comes to documentation. Writing a log on what is done, when its done, WHY it is done and the result/findings. The Airflow logs are documentation on that the pipeline ran, `quality_reports/` is the docs on WHAT happened and WHAT the result or findings were.

---

**Why this matters in practice(real life)?:**
- Lets say that a pipeline crashed during a Sunday night. I wake up on Monday. Without a report I only know that the pipeline crashed when watching the `airflow-status = FAIL`, but **WITH** a report Ill know *exactly* which contract that failed, how many rows `Silver` had, it makes it much easier to `debug` without having to trigger the pipeline again.

- Gradual degradation: if silver_row_count is slowly decreasing run by run, it is a signal before it breaks the contract. This is only visible if you have a history(logs) to track.

In upcoming **MVP v6** my `DuckDB` could technically ask the entire history with:  
`SELECT * FROM read_json('data/quality_reports/*.json')`  
And therefore my `reports` will become a datasource in of itself.

---

### What do I log and why:
This is what I intend to log when writing this document(MVP V4 - 16/06-2026)

| Field | Type | Why |
|------|-----|--------|
| `timestamp_utc` | ISO-string | Connects the report to Airflow execution |
| `status` | `"PASS"` / `"FAIL"` | Quick overview |
| `has_error_logs` | bool | Did Soda even run, or did config crash? |
| `duration_seconds` | float | Performance monitoring |
| `silver_row_count` | int | Volume at check |
| `silver_path` | String | Which path was reviewed? |

**Why JSON and not a log-file?:**
A log is text, I can read it but *not* ask it. `JSON` is structured which means `DuckDB` can run
```sql
SELECT AVG(duration_seconds) FROM read_json(.....)
```
against my entire report-hisroty without me having to write a line of parsing code. This is the difference between a "paper in a box" and a `parquet`-layer.

- I will stick to this file naming convention: `report_20260616_143022.json`

- The format `YYYMMDD_HHMMSS` is lexicographically sortable, i.e alphabetical order = chronological order. This means that this command: 
    - `ls data/quality_reports/ | tail -1`
    - Will ALWAYS give me the latest report and it works without having to mix in any external logic what so ever.

