import duckdb
# Räkna 'opened'-rader för apache/airflow, via staging-VIEW:en (genom dbt)

con = duckdb.connect("data/dbt/github_lake.duckdb", read_only=True)

result = con.sql("""
SELECT COUNT(*) AS via_staging_view
FROM stg_github_events
WHERE event_type = 'PullRequestEvent'
  AND event_action = 'opened'
  AND repo_name = 'apache/airflow' """)

print(result)
con.close()
