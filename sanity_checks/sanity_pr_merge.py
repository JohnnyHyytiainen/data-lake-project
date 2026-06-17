# Kommentarer: Svenska
# Kod: Engelska
# Sanity check script, se docs/file_docs/sanity_check_mvp5.md
import duckdb


import duckdb

# Verifiera att GROUP BY + MIN producerar exakt en rad per PR
result = duckdb.sql("""
    SELECT COUNT(*) AS unika_merged_prs
    FROM (
        SELECT
            repo_name,
            pr_number,
            MIN(created_at) AS closed_at
        FROM read_parquet('./data/silver/events/**/*.parquet', hive_partitioning=true)
        WHERE event_type = 'PullRequestEvent'
          AND (
              pr_action = 'merged'
              OR (pr_action = 'closed' AND pr_merged = True)
          )
        GROUP BY repo_name, pr_number
    )
""").df()

# Förväntat: ~78 047 (78 117 totala events minus 70 dubbletter = 78 047 unika PRs)
print(result)
