# function to use for diagnosing soda core issues.
# stand-alone script to save if diagnosing is further.
import duckdb
from loguru import logger

# Manuellt diagnostikverktyg — körs INTE av Airflow.
# Användning (Kör det som en enda lång rad utan backslashes, det är alltid säkrast i Git Bash på Windows):

# docker run --rm -v "C:/Users/johnn/Desktop/projekt/data-lake-project/quality:/app/quality" -v "C:/Users/johnn/Desktop/projekt/data-lake-project/data:/app/data" data-lake-project-quality python /app/quality/diagnose_soda.py

# PATHING - TODO: refactor och bryt ut dessa och skriv in i min config.py vid fungerande första PoC körning.
SILVER_PARQUET_GLOB = "/app/data/silver/events/**/*.parquet"
TEMP_DB_PATH = "/tmp/soda_silver.duckdb"
CHECKS_FILE = "/app/quality/checks/silver_checks.yml"
DATASOURCE_NAME = "silver_duckdb"


# Skapa en VIEW DuckDB view table
def create_duckdb_view() -> None:
    """
    Creates a temp DuckDB-file with a VIEW table over Silver Parquet-files.
    Soda Core speaks SQL - NOT parquet. The VIEW table is a bridge.
    DuckDB-connection closes before Soda opens the file (Single-writer-rule).
    """
    logger.info(f"Registering Silver VIEW in DuckDB: {TEMP_DB_PATH}")
    conn = duckdb.connect(TEMP_DB_PATH)
    conn.execute(f"""
                 CREATE OR REPLACE VIEW silver_events AS
                 SELECT * FROM read_parquet('{SILVER_PARQUET_GLOB}', hive_partitioning = true)
    """)
    conn.close()  # KRITISKT: Stäng INNAN Soda öppnar.
    logger.info("DuckDB VIEW created and connection is now closed.")


def diagnose_silver_data() -> None:
    """
    Kör varje kontraktsvillkor som direkta DuckDB-queries.
    Loggar exakt antal felande rader per check INNAN Soda startar.
    """
    logger.info("=== PRE-FLIGHT DIAGNOSTIK ===")
    conn = duckdb.connect(TEMP_DB_PATH, read_only=True)

    checks = {
        "row_count (ska vara > 0)": "SELECT COUNT(*) FROM silver_events",
        "null event_id (ska vara 0)": "SELECT COUNT(*) FROM silver_events WHERE event_id IS NULL",
        "null event_type (ska vara 0)": "SELECT COUNT(*) FROM silver_events WHERE event_type IS NULL",
        "null repo_name (ska vara 0)": "SELECT COUNT(*) FROM silver_events WHERE repo_name IS NULL",
        "null created_at (ska vara 0)": "SELECT COUNT(*) FROM silver_events WHERE created_at IS NULL",
        "duplicate event_id (ska vara 0)": "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM silver_events",
        "PullRequestEvent utan pr_action (ska vara 0)": "SELECT COUNT(*) FROM silver_events WHERE event_type = 'PullRequestEvent' AND pr_action IS NULL",
        "framtida timestamps (ska vara 0)": "SELECT COUNT(*) FROM silver_events WHERE TRY_CAST(created_at AS TIMESTAMP) > now()",
        "okänt pr_action-värde (ska vara 0)": """SELECT COUNT(*) FROM silver_events 
               WHERE pr_action IS NOT NULL 
               AND pr_action NOT IN ('opened','closed','merged','reopened','synchronize','ready_for_review')""",
    }

    for namn, query in checks.items():
        antal = conn.execute(query).fetchone()[0]
        logger.info(f"  {namn}: {antal}")

    result = conn.execute("""
        SELECT pr_action, COUNT(*) as antal
        FROM silver_events
        WHERE pr_action IS NOT NULL
        AND pr_action NOT IN ('opened','closed','merged','reopened','synchronize','ready_for_review')
        GROUP BY pr_action
        ORDER BY antal DESC
    """).fetchall()

    for rad in result:
        logger.info(f"  Okänd pr_action: '{rad[0]}' — {rad[1]} rader")

    conn.close()
    logger.info("=== DIAGNOSTIK KLAR ===")


if __name__ == "__main__":
    create_duckdb_view()  # återanvänd från run_checks.py
    diagnose_silver_data()
