# Own docs for airflow and Directed Acyclic Graphs (DAGs) and its importance.


## Simplified explanations on DAGs

De DAG diagram som visas vid sökning ser mer invecklade ut än vad det egentligen är. DAG akronymen står för **Directed Acyclic Graph** vilket i stora drag innebär ett mönster som jag redan är van vid att både bygga, tänka kring och jobba med. Enkelt förklarat med nuvarande konceptuell förståelse är detta:

- Med min nuvarande pipeline och de pipes jag tidigare byggt så flödar datan genom BRONZE -> SILVER -> GOLD. Varje pil pekar framåt, *ALDRIG* bakåt. Jag likadant som att datan alltid ska flöda framåt och inte t.ex från Silver till Bronze, dbt kan aldrig köras innan bronze har flyttat klart datan från bronze till silver. **DET** är en *DAG*.

- Det **DIRECTED** betyder är att att pilarna *HAR* en riktning.  
- Det **ACYCLIC** betyder är att det *ALDRIG* finns en cirkel. Ett steg kan aldrig trigga sig självt direkt eller indirekt.  
- Det **GRAPH** betyder är enbart det matematiska ordet för ett diagram, med nodes och kanter. 

    - **DAGs** är alltså ett flödesschema där pilarna *alltid* pekar framåt och *ALDRIG* bildar loopar. Det är "basically" allting. Rent konceptuellt dvs, i verkligheten är det en helt annan sak, att bygga det och förstå det på djupet är ett annat monster, det är därför jag ger mig på ett försök till att lyckas få det att fungera.

- I stora drag så ser det ungefär ut så här: 'Gör steg A, sen steg B och C parallelt, sen gör steg D när *BÅDE* steg B och C är klara.

## Manuell körning och krasch direkt.

Försök till att köra min pipe manuellt första gången via Airflow UI och jag stöter på röda "rutor" hela tiden.

### Fellog:

```Docker
 ▶ Log message source details
[2026-05-03, 17:28:47 UTC] {local_task_job_runner.py:123} ▶ Pre task execution logs
[2026-05-03, 17:28:47 UTC] {docker.py:345} INFO - Starting docker container from image apache/spark:3.5.3
[2026-05-03, 17:28:47 UTC] {docker.py:353} WARNING - Using remote engine or docker-in-docker and mounting temporary volume from host is not supported. Falling back to `mount_tmp_dir=False` mode. You can set `mount_tmp_dir` parameter to False to disable mounting and remove the warning
[2026-05-03, 17:28:50 UTC] {docker.py:69} INFO - Traceback (most recent call last):
[2026-05-03, 17:28:50 UTC] {docker.py:69} INFO -   File "/app/transforms/bronze_to_silver.py", line 7, in <module>
[2026-05-03, 17:28:50 UTC] {docker.py:69} INFO -     from loguru import logger
[2026-05-03, 17:28:50 UTC] {docker.py:69} INFO - ModuleNotFoundError: No module named 'loguru'
[2026-05-03, 17:28:50 UTC] {docker.py:69} INFO - 26/05/03 17:28:50 INFO ShutdownHookManager: Shutdown hook called
[2026-05-03, 17:28:50 UTC] {docker.py:69} INFO - 26/05/03 17:28:50 INFO ShutdownHookManager: Deleting directory /tmp/spark-c28b5e89-5d1f-4c6a-a237-5733871acb6d
[2026-05-03, 17:28:50 UTC] {taskinstance.py:3313} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/docker/api/client.py", line 275, in _raise_for_status
    response.raise_for_status()
  File "/home/airflow/.local/lib/python3.12/site-packages/requests/models.py", line 1024, in raise_for_status
    raise HTTPError(http_error_msg, response=self)
requests.exceptions.HTTPError: 400 Client Error: Bad Request for url: http+docker://localhost/v1.54/containers/create
The above exception was the direct cause of the following exception:
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/docker/operators/docker.py", line 350, in _run_image
    return self._run_image_with_mounts([*self.mounts, tmp_mount], add_tmp_variable=True)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/docker/operators/docker.py", line 377, in _run_image_with_mounts
    self.container = self.cli.create_container(
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/docker/api/container.py", line 440, in create_container
    return self.create_container_from_config(config, name, platform)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/docker/api/container.py", line 457, in create_container_from_config
    return self._result(res, True)
           ^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/docker/api/client.py", line 281, in _result
    self._raise_for_status(response)
  File "/home/airflow/.local/lib/python3.12/site-packages/docker/api/client.py", line 277, in _raise_for_status
    raise create_api_error_from_http_exception(e) from e
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/docker/errors.py", line 39, in create_api_error_from_http_exception
    raise cls(e, response=response, explanation=explanation) from e
docker.errors.APIError: 400 Client Error for http+docker://localhost/v1.54/containers/create: Bad Request ("invalid mount config for type "bind": bind source path does not exist: /tmp/airflowtmppo9zg524")
During handling of the above exception, another exception occurred:
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 768, in _execute_task
    result = _execute_callable(context=context, **execute_callable_kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 734, in _execute_callable
    return ExecutionCallableRunner(
           ^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/utils/operator_helpers.py", line 252, in run
    return self.func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/baseoperator.py", line 424, in wrapper
    return func(self, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/docker/operators/docker.py", line 485, in execute
    return self._run_image()
           ^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/docker/operators/docker.py", line 359, in _run_image
    return self._run_image_with_mounts(self.mounts, add_tmp_variable=False)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/docker/operators/docker.py", line 419, in _run_image_with_mounts
    raise DockerContainerFailedException(f"Docker container failed: {result!r}", logs=log_lines)
airflow.providers.docker.exceptions.DockerContainerFailedException: Docker container failed: {'StatusCode': 1}
```
**Hypotes kring fel:**
- Saknade deps någonstans pga början av `ModuleNotFoundError` i början.
- Generisk "catch all" felkod med `"StatusCode": 1` som oftast tyder på att jag har gjort någonting fel i min docker-compose, min Dockerfile(Tveksamt), någon image är fel-mounted, eller några issues med pathing gremlins.

    - Men `ModuleNotFoundError` låter helt galet att det hänger ihop med ett "generiskt" fel?? Måste vara två issues som spökar nu vid körning av allting via Airflow UI... 

    - Första felet verkar ha med spark att göra. Kan det vara pga att spark måste isoleras i min Docker container så som dbt görs? Isolera det ännu mer så min laptops UV miljö är HELT separat ifrån min docker container? (Troligen) Men det makes no sense då jag redan mountar en spark image som "stand-alone" i min docker-compose.yml(?!) Lösningen var att enbart uppdatera docker-compose.yml som står om nedan.

    - Nästa issue: DockerOperator och `mount_tmp_dir=False,` vilket var en "snabb" fix i mitt `github_lake_dag.py` script. 

Hela lösningen för båda problemen är: Ännu en Dockerfile.spark, uppdaterade rader i DAGs scriptet och uppdatering i docker-compose.yml filen. Kritiskt att förstå master/worker förhållanden i docker-compose och Dockerfiler. Tillbaka till teorin för att förstå ännu mer då jag inte riktigt greppat allting ännu, men nu verkar det fungera genom att enbart ha ersatt: `image: apache/spark:3.5.3` i docker-compose.yml filen med:

```
build:
    context: .
    dockerfile: Dockerfile.spark
```

## Issue nummer två efter att ha löst ModuleNotFoundError med dependencies.
Ännu ett issue jag stötte på, kom längre i min manuella körning av min DAG i Airflow UI't. Nu pekar det mot mina konstanter i `config.py` filen. Vilket är ännu mer konstigt, deps är mer logiskt vs min egen lokala config fil. Kan problemet ligga vid just LOKALA config filen?`

### Fellog
```docker
[2026-05-03, 18:33:57 UTC] {local_task_job_runner.py:123} ▶ Pre task execution logs
[2026-05-03, 18:33:57 UTC] {docker.py:345} INFO - Starting docker container from image data-lake-project-spark
[2026-05-03, 18:34:00 UTC] {docker.py:69} INFO - Traceback (most recent call last):
[2026-05-03, 18:34:00 UTC] {docker.py:69} INFO -   File "/app/transforms/bronze_to_silver.py", line 21, in <module>
[2026-05-03, 18:34:00 UTC] {docker.py:69} INFO -     from config import (
[2026-05-03, 18:34:00 UTC] {docker.py:69} INFO - ModuleNotFoundError: No module named 'config'
[2026-05-03, 18:34:00 UTC] {docker.py:69} INFO - 26/05/03 18:34:00 INFO ShutdownHookManager: Shutdown hook called
[2026-05-03, 18:34:00 UTC] {docker.py:69} INFO - 26/05/03 18:34:00 INFO ShutdownHookManager: Deleting directory /tmp/spark-8a4d7359-0775-46b7-ab60-02558d1dd7f1
[2026-05-03, 18:34:00 UTC] {taskinstance.py:3313} ERROR - Task failed with exception
Traceback (most recent call last):
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 768, in _execute_task
    result = _execute_callable(context=context, **execute_callable_kwargs)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/taskinstance.py", line 734, in _execute_callable
    return ExecutionCallableRunner(
           ^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/utils/operator_helpers.py", line 252, in run
    return self.func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/models/baseoperator.py", line 424, in wrapper
    return func(self, *args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/docker/operators/docker.py", line 485, in execute
    return self._run_image()
           ^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/docker/operators/docker.py", line 362, in _run_image
    return self._run_image_with_mounts(self.mounts, add_tmp_variable=False)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/airflow/.local/lib/python3.12/site-packages/airflow/providers/docker/operators/docker.py", line 419, in _run_image_with_mounts
    raise DockerContainerFailedException(f"Docker container failed: {result!r}", logs=log_lines)
airflow.providers.docker.exceptions.DockerContainerFailedException: Docker container failed: {'StatusCode': 1}
[2026-05-03, 18:34:00 UTC] {taskinstance.py:1226} INFO - Marking task as FAILED. dag_id=github_lake_pipeline, task_id=bronze_to_silver, run_id=manual__2026-05-03T18:28:52.150064+00:00, execution_date=20260503T182852, start_date=20260503T183357, end_date=20260503T183400
[2026-05-03, 18:34:00 UTC] {taskinstance.py:341} ▶ Post task execution logs
```
**Hypotes kring felet:**
- Eftersom att jag löste Deps issues senast men ändå får `ModuleNotFoundError` ifrån min `config`-fil med alla mina konstanter bör väl svaret vara att jag måste mounta min config fil också, likadant som att jag mountar `transforms/` foldern i containern(?). Istället för att försöka skala av varje lager av fel bör jag nog kunna strunta i att mounta min `transforms/` folder och bara köra *HELA* jäkla `rooten`? (Famous last words innan jag läcker min API key och alla secrets LOL)
    
    - Nej, vid närmare eftertanke så ska jag inte vara lat och ha med hela ROOT enbart pga lazy. Har fått det bekräftat av både stackoverflow, Gemini och Claude att det går att mounta hela projektets root. Vilket jag ej vill göra. Jag tar den jobbiga vägen istället. 

- Lösningen: **Principle of last privilege**, jag ger min container tillgång till EXAKT det den behöver, inte mer. Jag får fortsätta skala av lager för lager tills allting funkar med minst tillåtelser möjligt. Lösningen på detta issue är enbart uppdatera mitt DAGs script med: 
```python
        environment={
            "PYTHONPATH": "/app",
        },
                # Mounts som ersätter volumes i DockerOperator, samma logik som volumes i compose
        mounts=[
            Mount(
                source="/c/Users/johnn/Desktop/projekt/data-lake-project/data",
                target="/app/data",
                type="bind",
            ),
            Mount(
                source="/c/Users/johnn/Desktop/projekt/data-lake-project/transforms",
                target="/app/transforms",
                type="bind",
            ),
            Mount(
                source="/c/Users/johnn/Desktop/project/data-lake-project/config.py",
                target="/app/config.py",
                type="bind",
            ),
        ],
```
- Jag lägger till `environment={"PYTHONPATH": "/app",}` samt en ny Mount i i min `run_silver = DockerOperator`
- Det gick, fram tills **NÄSTA** `ModuleNotFoundError`, och det är nu det "läskiga" börjar. Behöver rådfråga och särskilja extra mycket på `FileNotFoundError:` och `ModuleNotFoundError:`.
    - Skillnaden mellan `FileNotFoundError` VS `ModuleNotFoundError` är "enkel"(På min nuvarande nivå av förståelse). FileNotFoundError pekar på att FILEN saknas och jag måste mounta den medan ModuleNotFoundError tyder på att det **INTE** är själva filen som saknas utan **DEPENDENCIES** som saknas.


## Issue nummer tre efter att ha löst ModuleNotFoundError med config.py scriptet.
- Ännu ett issue efter config ej hittades. Dvs detta:

```docker
[2026-05-03, 19:20:32 UTC] {docker.py:69} INFO -     from dotenv import load_dotenv
[2026-05-03, 19:20:32 UTC] {docker.py:69} INFO - ModuleNotFoundError: No module named 'dotenv'
[2026-05-03, 19:20:32 UTC] {docker.py:69} INFO - 26/05/03 19:20:32 INFO ShutdownHookManager: Shutdown hook called
[2026-05-03, 19:20:32 UTC] {docker.py:69} INFO - 26/05/03 19:20:32 INFO ShutdownHookManager: Deleting directory /tmp/spark-51b43d12-b69d-42ec-a8e7-57b44ac24798
```
- Lösningen på detta issue bör alltså vara -> Lägg till deps i min Dockerfile.spark. Fortsätter dessa issues så bör jag se över om det är bättre med en `requirements.txt` fil istället med alla deps jag använder mig utav. 

- För att underlätta mina "framtida" 3-5 körningar uppdaterar jag nuvarande `Dockerfile.spark` till denna:

```Dockerfile
# Dockerfile för spark.
# En officiell Spark image med mina projekts python deps
# BÖR läsa ModuleNotFoundError när Airflow ska köra bronze to silver pipen
# OM jag nu ändrar min DockerOperator också i github_lake_dag.py scriptet
FROM apache/spark:3.5.3

USER root

# installerar pip och sen projektets deps direkt
# Kopierar i stora drag bara min pyproject.toml fil och inte hela koden,
# Koden bör mountas via volumes när jag kör allt
RUN apt-get update && apt-get install -y python3-pip && \
    pip install \
      loguru \
      pandas \
      pyarrow \
      python-dotenv \
      pyspark==3.5.3

USER spark
```
- Förhoppningsvis så underlättar detta min framtida huvudvärk och slippa spela 'whack-a-mole' med mina dependencies. Dags för en
    - `docker-compose build spark`
    - `docker-compose up -d --force-recreate spark airflow-scheduler`

## Issue nummer fyra efter att ha löst ModuleNotFoundErrors:
Äntligen ett steg längre på vägen. Issue jag stöter på nu är detta:
```docker
[2026-05-03, 19:44:20 UTC] {docker.py:69} INFO -   File "/app/transforms/bronze_to_silver.py", line 49, in <module>
[2026-05-03, 19:44:20 UTC] {docker.py:69} INFO -     def _load_checkpoint() -> set[str]:
[2026-05-03, 19:44:20 UTC] {docker.py:69} INFO - TypeError: 'type' object is not subscriptable  
```
- Problemet: "standard" python compatability problem. set[str] som en typehint kräver python 3.9+ medan Spark containern använder en äldre version. Fixen BÖR vara `from __future__ import annotations` för att kunna kringgå detta.