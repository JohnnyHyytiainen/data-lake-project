# DAGs script - Orkestrera körning med Pyspark Bronze -> Silver
# Köra dbt Silver -> Gold (marts). Kör HELA min Data lake pipeline.
# DAG ansvarar för Silver transformation och dbt Gold layer.
# Kommentarer: Svenska
# Kod: Engelska

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Standard argumentet som ärvs av ALLA mina tasks i DAG.
# Misslyckas en task -> vänta 5 min och försök sen igen. MAX 1 retry
default_args = {"owner": "johnny", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="github_lake_pipeline",
    description="Bronze -> Silver via PySpark, Silver -> Gold via dbt",
    # Cron syndax - minut, timme, dag, månad, veckodag
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    # catchup = False: Kör inte igen för alla dagar sedan start_date, annars försöker Airflow köra pipen varje dag sen 26-01-01
    catchup=False,
    default_args=default_args,
    tags=["data-lake", "portfolio"],
) as dag:
    # TASK 1) PySpark, Bronze -> Silver layer. DockerOperator startar ny container baserat på Spark image
    # Silver transformation och stänger container when done. Nytt bygge behövs ej.
    run_silver = DockerOperator(
        task_id="bronze_to_silver",  # Image som redan finns lokalt ifrån docker-compose builden
        image="data-lake-project-spark",
        user="root",  # Kör som root, samma som i docker-compose för Spark Service
        command=[
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[*]",
            "/app/transforms/bronze_to_silver.py",
        ],
        environment={
            "PYTHONPATH": "/app",
        },
        # Mounts som ersätter volumes i DockerOperator, samma logik som volumes i compose
        mounts=[
            Mount(
                source="/c/Users/johnn/Desktop/projekt/data-lake-project/transforms",
                target="/app/transforms",
                type="bind",
            ),
            Mount(
                source="/c/Users/johnn/Desktop/projekt/data-lake-project/data",
                target="/app/data",
                type="bind",
            ),
            Mount(
                source="/c/Users/johnn/Desktop/projekt/data-lake-project/config.py",
                target="/app/config.py",
                type="bind",
            ),
        ],
        # Container tas bort per automatik när task är klar, likadant som --rm
        mount_tmp_dir=False,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="data-lake-network",
    )

    # TASK 2) dbt, Silver -> Gold layer. DockerOperator bygger på dbt imagen och kör dbt run
    # Samma som docker-compose run --rm dbt MEN triggas av Airflow ist för manuellt av mig
    run_dbt = DockerOperator(
        task_id="silver_to_gold_dbt",
        image="data-lake-project-dbt",
        command=["dbt", "run"],
        environment={
            "DBT_PROFILES_DIR": "/app/dbt",
        },
        mounts=[
            Mount(
                source="/c/Users/johnn/Desktop/projekt/data-lake-project/dbt",
                target="/app/dbt",
                type="bind",
            ),
            Mount(
                source="/c/Users/johnn/Desktop/projekt/data-lake-project/data",
                target="/app/data",
                type="bind",
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode="data-lake-network",
        working_dir="/app/dbt",
    )

    # BEROENDEN: Silver _MÅSTE_ vara klar innan dbt ska starta. Definitionen av DAG, aldrig bakåt.
    # I Airflow UI ser det ut som två nodes med en pil mellan
    run_silver >> run_dbt
