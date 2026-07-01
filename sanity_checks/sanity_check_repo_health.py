# Sanity check script för att validera repo_health mart mot riktig materialized data.
# repo_health är en TABLE och INTE en VIEW. Ingen os.chdir() behövs här.

import duckdb

# paths, navigera från sanity_checks/ till root -> till DuckDB fil
con = duckdb.connect("data/dbt/github_lake.duckdb", read_only=True)

# ===== 1: Total population =====
# Hur många repos kvalificerar i alla 3 komponenter?
# För litet (< ~30) gör percentile-rankings instabila och extremt svårtolkade
print("===== 1: Qualifying repo population =====")
print(con.execute("SELECT COUNT(*) AS repo_count FROM repo_health").fetchdf())


# ===== 2: Score distribution =====
# repo_health_score bör ligga inom [0, 1] och ändå vara rimligt spridd
# INTE klumpad mot 0 ELLER 1. Genomsnitt nära 0.5 med spridning över HELA spannet är
# ett bra tecken på hälsan och att normaliseringen fungerar som tänkt.
print("\n===== 2: Health score distribution =====")
print(
    con.execute("""
    SELECT
        ROUND(min(repo_health_score), 3) AS min_score,
        ROUND(avg(repo_health_score), 3) AS avg_score,
        ROUND(max(repo_health_score), 3) AS max_score,
        ROUND(quantile_cont(repo_health_score, 0.25), 3) AS p25,
        ROUND(quantile_cont(repo_health_score, 0.50), 3) AS p50,
        ROUND(quantile_cont(repo_health_score, 0.75), 3) AS p75,
        ROUND(quantile_cont(repo_health_score, 0.95), 3) AS p95,
    FROM repo_health
""").fetchdf()
)


# ===== 3: 0.0 timmar klustret, FULLSTÄNDIG lista =====
# Dom här repos delar pr_speec_score = 1.000 pga PERCENT_RANK-tie vid MINSTA värdet.
# Frågan här är: Är det legit snabba repos, keyword-noise ELLER suspected_automation som
# har sluppit igenom is_bot = false filtret..
print("\n===== 3: Repos and median_pr_hours = 0.0 =====")
print(
    con.execute("""
    SELECT
        repo_name,
        pr_count,
        median_pr_hours,
        total_commits,
        total_stars,
        repo_health_score
    FROM repo_health
    WHERE median_pr_hours = 0.0
    ORDER BY pr_count DESC
""").fetchdf()
)

# ===== 4: Redan kända DE-repos, "spot check" =====
# Dom här ska finnas i tabellen och ha rimliga värden.
# apache/airflow: HÖG pr_count, LÅG median_hours (Känt open source community repo med LÅNG review)
# dbt-labs/dbt-core: medelstort, "måttlig" cykeltid.
# trinodb/trino: Liknande profil som AIRFLOW.
print("\n===== 4: Known DE-repos =====")
print(
    con.execute("""
    SELECT
        repo_name,
        pr_count,
        median_pr_hours,
        total_commits,
        total_stars,
        pr_speed_score,
        commit_volume_score,
        star_growth_score,
        repo_health_score
    FROM repo_health
    WHERE repo_name IN (
        'apache/airflow',
        'pola-rs/polars',
        'trinodb/trino',
        'dbt-labs/dbt-core',
        'PrefectHQ/prefect',
        'duckdb/duckdb-r',
        'apache/spark',
        'apache/kafka'
    )
    ORDER BY repo_health_score DESC
""").fetchdf()
)

con.close()
