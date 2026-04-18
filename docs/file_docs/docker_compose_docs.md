# Own docs for Docker, docker-compose.yml and containerization
Nästa steg i roadmappen är containerization av `PySpark`.
Det innebär att det är dags att börja utforma min `docker-compose.yml`-fil och fylla i den.


Tankar om ordningen i `ROADMAP.md` är att ordningen spelar roll. Den bästa ordningen för att få klart MVP v3 är denna:
**Docker** -> **dbt** -> **Airflow** -> **Grafana dashboard**.

Varför? Jo för att varje steg är beroende av det tidigare steget. `Docker` kommer först eftersom att `Airflow` OCH `Grafana` lever i containers. De finns inte *utan* dom. Att försöka sätta upp `Airflow` utan att förstå `Docker-nätverket` och hur mina containers pratar med varandra.

DBT kommer direkt efter Docker av en enkel anledning: dbt producerar Gold tabellerna och Airflow ska orkestrera dbt. Det skulle vara bakåtvänt att bygga ett hus innan ens ha tittat på ritningarna för huset.. Sen av det jag läst så är dbt relativt fristående och enkelt att testa manuellt. Genom att jag kör en modell, kollar outputen, itererar. Airflow kommer efter dbt eftersom att det binder ihop hela Pipen. En DAG i Airflow är som ett recept. 'Kör `bronze_to_silver`, vänta tills det är klart för att sen köra `dbt`, vänta tills det är klart. MEN utan `dbt`-modellerna är `Airflow` meningslöst. Till sist är det `Grafana`-time. MEN för att `Grafana` ska kunna användas så kräver det att tidigare steg är på plats.


## PySpark & Docker.
**Något viktigt att förstå innan Jag ens rör en enda rad i `docker-compose.yml`:** 

- PySpark är inte som mina andra services. Min producer och consumer är rena Python processer, dom passar perfekt i min `python:3.12-slim` image. PySpark kräver dock Java under huven, Spark är skrivet i Scala och körs på en JVM (Java Virtual Machine).  

Python-gränssnittet jag använder är egentligen bara ett skal som pratar med min JVM. Det betyder att jag inte bara "kan stoppa in PySpark" i min nuvarande Dockerfile utan att det blir en huvudvärk. Jag kommer behöva skriva en separat image byggd för just de syftet

Exempel: Min nuvarande Dockerfile är en lägenhet inredd för en Python utvecklare. PySpark containern är ett helt annat hus som råkar ligga på samma gata, min `docker-compose.yml` är gatan i detta exempel. PySpark behöver en egen lägenhet(Dockerfile) inredd för sig.

Efter research och rådfrågande av LLM(Claude Sonnet 4.6) så verkar det naturliga valet här vara `bitnami/spark`, vilket är en välunderhållen image som paketerar Spark med Java och allt annat som behövs utan att jag ska behöva bygga det från grunden. Det lär spara mig tid och en enorm huvudvärk.

---
Viktig info att förstå är hur Spark faktiskt startas. Spark har något som heter standalone mode, en Spark-instans som är sin egen master och worker på samma gång, utan ett externt kluster. Av min förståelse och för mitt MVP är standalone mer än tillräcklig.

Viktigt att lägga märke till i nuvarande `docker-compose.yml` är att bara consumer har en volume mount: `./data:/app/data` Det är consumer som skriver Bronze-filer till disk. PySpark-containern behöver läsa samma filer och skriva Silver och Gold tillbaka till samma mapp. Volymen är alltså **nyckelkomponenten** som kopplar ihop hela pipeline flödet.  

Nätverket data-lake-network är redan definierat och `bitnami/spark` kommer automatiskt att ansluta till det eftersom det är mitt default network. Det BÖR jag inte behöva inte konfigurera manuellt.

Detta är vad som ska skrivas in i min docker-compose.yml direkt efter `consumer`-blocket och före mitt `network`-block:

```docker
# ======== PySpark (Standalone mode, tillräckligt för min MVP) ========
  spark:
    image: bitnami/spark:3.5          # Inkluderar Java + Spark, kör på Linux (löser winutils-problemet jag har)
    container_name: spark
    environment:
      - SPARK_MODE=master             # Standalone: samma container är både master och worker
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
    ports:
      - "8080:8080"                   # Spark Web UI - Jag kan se mina jobb i webbläsaren här
      - "7077:7077"                   # Spark master port - hit ansluter workers (och senare Airflow)
    volumes:
      - ./data:/app/data              # Samma bind mount som consumer - Bronze in, Silver/Gold ut
```