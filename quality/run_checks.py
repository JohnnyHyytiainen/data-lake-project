# quality/run_checks script
# Kommentarer: Svenska
# Kod: Engelska
# Entry point för soda core quality checks mot Silver parquet layer. Körs som Airflow DockerOperator-task:
# quality_check_silver
# Exit 0 = ALLA kontroller passed --> Airflow fortsätter till dbt
# Exit 1 = Minst ETT kontrakt bröts -> Airflow STOPPAR DAG'en

import sys
import duckdb
from loguru import logger
from soda.scan import Scan

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


# Kör min Soda 'scan' mot DuckDB VIEW table som skapats
def run_soda_scan() -> bool:
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
    logger.debug(
        f"Soda config thats being sent:\n{config_yaml}"
    )  # Blir synlig i Airflow loggen

    scan.add_configuration_yaml_str(config_yaml)
    scan.add_sodacl_yaml_file(CHECKS_FILE)
    scan.set_verbose(True)
    scan.execute()

    # Guard 1: konfigurationsfel (trasig YAML, saknad datasource, etc.)
    # has_errors() fångar det Soda loggade men scan.has_check_fails() missade
    if scan.has_error_logs():
        logger.error(
            "Soda scan has got configuration issues - No checks were ran correctly.."
        )
        return False

    return not scan.has_check_fails()


def main() -> None:
    create_duckdb_view()
    passed = run_soda_scan()

    if passed:
        logger.success("[PASS] All Silver checks approved. Starting Gold-layer (dbt)")
        sys.exit(0)
    else:
        logger.error(
            "[FAIL] Data Quality contracts broken. Gold-layer (dbt) will NOT start."
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
