# Quick run commands for docker thats good to have.

* To build container:
    * ` docker-compose up --build `
* To build it "detatched" add -d before build:
    * ` docker-compose up -d --build ` 

* To start container:
    * ` docker-compose up -d `

* To nuke it all and rebuild it:
    * ` docker-compose down -v `

---

## To verify that the data is flowing

--- 
* Check logs, does the producer see the github events?
    * ` docker logs producer --tail 20 `

* Check consumer, does it write parquet to disk?
    * ` docker logs consumer --tail 20 `

* Check that Bronze folder is filling up:
    * ` find data/bronze -name "*.parquet" | head -10 `

* Start up `spark` within your docker container after adding it to your docker-compose.yml file:
    * ` docker compose up spark -d `

* To see your Spark container in `Spark Web UI`
    * `localhost:8080` in your browser.

* To verify that Spark can REACH my data, that is, my volume ` ./data:/app/data ` works properly. I can run this command inside my spark container.
    - ` ls /app/data `

--- 

* To run DBT form my docker container I use this command:
    * ` docker-compose run dbt ` <- This command leaves a useless container left after running the gold dbt aggregations

    * ` docker-compose run --rm dbt ` <- This command REMOVES the container after gold aggregations are done. This is the one to run.

--- 

* To build new spark image:
    * ` docker-compose build spark ` <-- This builds it
    * ` docker-compose up -d --force-recreate spark ` <-- FORCES recreation.

--- 
* To build new airflow scheduler:
    * ` docker-compose up -d --force-recreate airflow-scheduler `

--- 
* To build my Quality image (`Dockerfile.quality`)
    * ` docker build -f Dockerfile.quality -t data-lake-project-quality . `

* VERIFY that the quality image exists:
    * ` docker images | grep quality `

* Once verified that the `quality`-image exists, restart the Airflow-scheduler so that it picks up the changed made in the `DAGs` order:
    * `docker compose restart airflow-scheduler `

---

## To run diagnose_soda.py script:

* To run diagnostics script on soda(`diagnose_soda.py`) use this command in bash terminal:

    * ` MSYS_NO_PATHCONV=1 docker run --rm -v "C:/Users/johnn/Desktop/projekt/data-lake-project/quality:/app/quality" -v "C:/Users/johnn/Desktop/projekt/data-lake-project/data:/app/data" data-lake-project-quality python /app/quality/diagnose.py `

- Reason its with `MSYS_NO_PATHCONV=1` is because of normal Git Bash issues. Git bash is trying to be helpful and converts `/app/quality/diagnose_soda.py` to a windows path automatically. That is why I can see `C:/Program Files/Git/app/quality/diagnose_soda.py` in the error message.



## To rebuild my quality circuit breaker

* To rebuild my circuit breaker and test it:
    * ` docker compose build quality `
    * ` docker compose run --rm quality `

* To verity that the repport landed locally on the host machine:
    * ` ls -la data/quality_reports/ `
    * ` cat data/quality_reports/$(ls data/quality_reports/ | tail -1) `



## MANUAL TRIGGERS
dbt och quality har `profiles: manual`, de startas ALDRIG av `docker compose up -d`.
I normal drift triggas de av Airflow DAGen automatiskt.

### Kör isolerat (för testning utan Airflow)
* ` docker compose run --rm dbt `        <- kör dbt run mot Silver till Gold
* ` docker compose run --rm quality `    <- kör Soda scan mot Silver Parquet

### Bygg om efter Dockerfile-ändringar
* ` docker compose build dbt `           <- rebuild dbt-imagen
* ` docker compose build quality `       <- rebuild quality-imagen

### Efter nuke, rätt ordning
1. ` docker compose up -d `            <- startar Kafka, Spark, Airflow, Grafana
2. Trigga DAGen manuellt i Airflow UI (localhost:8081)
   ELLER kör isolerat för snabbtest:
   docker compose run --rm quality
   docker compose run --rm dbt


##  Examples on my manual triggers:

* Re-build my dbt:
    * ` docker compose run --rm dbt `       <- kör dbt, bygger inte om
    * ` docker compose build quality `      <- bygger quality-imagen, ej dbt

* Re-build my quality:
    * ` docker compose run --rm quality `   <- kör quality, bygger inte om  
    * ` docker compose build dbt `          <- bygger dbt-imagen, ej quality
