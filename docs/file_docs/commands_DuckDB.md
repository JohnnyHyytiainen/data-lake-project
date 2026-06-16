# Duckdb commands to have that is useful.
Docs regarding `DuckDB`-commands that are useful for my project.

* Identify what columns my silver layer actually contains(run from root):
    * ` duckdb -c "DESCRIBE SELECT * FROM read_parquet('data/silver/events/**/*.parquet') LIMIT 1;" `

- I should see this from running command above in terminal:


| column_name | column_type | null | key | default | extra |
|:---|:---|:---|:---|:---|:---|

|varchar |varchar | varchar | varchar | varchar | varchar |
|:---|:---|:---| :--- | :--- | :--- |
| event_id     | VARCHAR     | YES     | NULL    | NULL    | NULL    |
| event_type   | VARCHAR     | YES     | NULL    | NULL    | NULL    |
| actor_login  | VARCHAR     | YES     | NULL    | NULL    | NULL    |
| repo_name    | VARCHAR     | YES     | NULL    | NULL    | NULL    |
| repo_id      | VARCHAR     | YES     | NULL    | NULL    | NULL    |
| commit_count | INTEGER     | YES     | NULL    | NULL    | NULL    |
| pr_number    | INTEGER     | YES     | NULL    | NULL    | NULL    |
| pr_action    | VARCHAR     | YES     | NULL    | NULL    | NULL    |
| pr_merged    | BOOLEAN     | YES     | NULL    | NULL    | NULL    |
| created_at   | TIMESTAMP   | YES     | NULL    | NULL    | NULL    |
| day          | VARCHAR     | YES     | NULL    | NULL    | NULL    |
| month        | VARCHAR     | YES     | NULL    | NULL    | NULL    |
| year         | BIGINT      | YES     | NULL    | NULL    | NULL    |
