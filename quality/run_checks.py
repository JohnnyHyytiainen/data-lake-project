# quality/run_checks script
# Kommentarer: Svenska
# Kod: Engelska
# Entry point för soda core quality checks mot Silver parquet layer. Körs som Airflow DockerOperator-task:
# quality_check_silver
# Exit 0 = ALLA kontroller passed --> Airflow fortsätter till dbt
# Exit 1 = Minst ETT kontrakt bröts -> Airflow STOPPAR DAG'en

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
import duckdb
from loguru import logger
from soda.scan import Scan

# ========== PATHING ==========
# TODO (tech-debt, inför MVP v6 cleanup sprint):
# Ersätt hårdkodade paths med import från config.py.
# Kräver: montera config.py i Dockerfile.quality + github_lake_dag.py DockerOperator.
# Prioritera ej förrän quality-lagret är stabilt över flera MVP-faser.
# =============================
SILVER_PARQUET_GLOB = "/app/data/silver/events/**/*.parquet"
SILVER_BASE_PATH = "/app/data/silver/events"  # <--- NY: För rapporten UTAN glob
TEMP_DB_PATH = "/tmp/soda_silver.duckdb"
CHECKS_FILE = "/app/quality/checks/silver_checks.yml"
DATASOURCE_NAME = "silver_duckdb"
QUALITY_REPORTS_DIR = (
    "/app/data/quality_reports"  # <--- NY: Destination för JSON-rapporter
)


# ========== VIEW DUCKDB ==========
# Skapa en VIEW DuckDB view table
# =================================
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


# ========== Soda SCAN MOT DUCKDB VIEW ==========
# Kör min Soda 'scan' mot DuckDB VIEW table som skapas
# I create_duckdb_view() funktionen
# ===============================================
def run_soda_scan() -> Scan:
    """
    Runs the Soda scan towards created DuckDB file containing VIEW table.
    Returns True if ALL controls pass.
    Returns False if ONE fails.
    """
    scan = Scan()
    scan.set_data_source_name(DATASOURCE_NAME)

    # Explicit string concatenation istället för f-string med indentation issues.
    # Varje rad börjar EXAKT på column 0 i den slutliga YAML-strängen.
    config_yaml = (
        f"data_source {DATASOURCE_NAME}:\n  type: duckdb\n  path: {TEMP_DB_PATH}\n"
    )
    logger.debug(f"Soda config thats being sent:\n{config_yaml}")

    scan.add_configuration_yaml_str(config_yaml)
    scan.add_sodacl_yaml_file(CHECKS_FILE)
    scan.set_verbose(True)
    scan.execute()

    return scan


# ========== QUALITY REPORT ==========
# Funktion för att skriva min quality report
# ====================================
def write_quality_report(scan: Scan, duration_seconds: float) -> Path:
    """
    Writes a JSON-report to QUALITY_REPORTS_DIR after every Soda run.
    ALWAYS called even if PASS or FAIL for historical traceability.
    The filename is lexicographically sortable: report_YYYYMMDD_HHMMSS.json
    """
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Räknar silver-rader DIREKT via DuckDB (NY anslutning då den temporära ovan är stängd)
    silver_row_count = 0
    try:
        conn = duckdb.connect()
        result = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{SILVER_PARQUET_GLOB}', hive_partitioning = true)"
        ).fetchone()
        silver_row_count = result[0] if result else 0
        conn.close()
    except Exception as e:
        logger.warning(f"Could NOT count silver-rows for report: {e}")

    report = {
        "run_id": run_id,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "FAIL"
        if (scan.has_check_fails() or scan.has_error_logs())
        else "PASS",
        "has_error_logs": scan.has_error_logs(),
        "duration_seconds": round(duration_seconds, 2),
        "silver_row_count": silver_row_count,
        "silver_path": SILVER_BASE_PATH,
        "checks_file": CHECKS_FILE,
    }

    reports_dir = Path(QUALITY_REPORTS_DIR)
    reports_dir.mkdir(parents=True, exist_ok=True)  # Skapa om ej existerar

    report_path = reports_dir / f"report_{run_id}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    logger.info(f"Quality report written: {report_path}")
    return report_path


def main() -> None:
    create_duckdb_view()  # Skapar DuckDB view table

    start_time = time.monotonic()  # Startar klockan innan scan
    scan = run_soda_scan()  # Tar emot scan-objekt
    duration = time.monotonic() - start_time  # Stoppar klockan

    write_quality_report(scan, duration)  # NY: skriv rapport (alltid, oavsett status)

    # Exakt samma exit-logik som innan, bara att källan nu är ett scan-objektet direkt
    if scan.has_error_logs() or scan.has_check_fails():
        logger.error(
            "[FAIL] Data Quality contracts broken. Gold-layer (dbt) will NOT start."
        )
        sys.exit(1)

    logger.success("[PASS] All Silver checks approved. Starting Gold-layer (dbt)")
    sys.exit(0)


if __name__ == "__main__":
    main()
