# github-data-lake - Planering MVP v4 -> v6
*Skapad: 2026-05-17 | Planning-session*

---

## Filosofin bakom sekvensen

Varje MVP-fas i det här projektet är designad så att den gör nästa fas bättre, inte bara större. MVP v4 bygger ett fundament av tillförlitlighet och kunskap som MVP v5's djupare analys kan stå på. MVP v5 bygger ett rikt och korrekt Golden layer som MVP v6's frågeställningar faktiskt kan ge korrekta svar på. Det är ett system som förhoppningsvis mognar med min kunskapsnivå, inte ett system som bara växer för växandets skull.

> "Quality controls i v4 gör att bot-klassificeringen i v5 bygger på data du litar på. En rik och pålitlig Gold-layer i v5 gör att RAG-lagret i v6 faktiskt kan ge korrekta svar."

---

## Cleanup Sprint - Täpp till hålen i MVP v3 innan v4 påbörjas

Syfte: Betala av den tekniska skulden från MVP v3. Varje känd brist som
lämnas öppen är ett lån med växande ränta, de påverkar varje körning,
varje test och varje framtida beslut om Jag inte stänger dem.

Resultatet av cleanup-sprinten taggas som `v3.0.1`.

### Infrastruktur-skulden - Checkpoint-filen

Problemet är att `DockerOperator` i Airflow spawnar Spark-containern via
host Dockerns daemon, inte via Airflow-containerns filsystem. Det innebär
att relativa sökvägar i mount-konfigurationen tolkas relativt till
host-maskinen, inte relativt till projektet inuti Airflow-containern. Följden
är att checkpoint-filen aldrig hittas, och varje DAG-körning är i praktiken
en cold run som processar om alla Bronze-filer från grunden vilket är extremt
onödigt och skapar issues. Ännu mer om jag tänker i kostnadsfrågor i riktig produktion.
Det är en overhead som kan bli dyr om det inte fixas i prod.

- [x] Identifiera exakt vilken mount-konfiguration som orsakar problemet
      (logga ut de faktiska sökvägarna som host-Docker försöker mounta)

- [x] Sätt `PROJECT_ROOT` som absolut sökväg i `.env` och `.env.example`

- [x] Uppdatera `DockerOperator`-mounts i DAGen att använda `PROJECT_ROOT`
      som prefix för alla mount-sökvägar

- [x] Verifiera att en körning faktiskt läser checkpoint och hoppar över
      redan processade filer (se logg: "X already processed")

- [ ] Dokumentera lösningen i `AC_CTXT` under kända
      begränsningar, förklara host-Docker vs container-Docker distinktionen.

### Testskulden - Återinför unit tests för PySpark-logiken

PySpark-migrationen i MVP v2 kraschade befintliga unit tests och de togs bort
ur CI-pipelinen. Idag kör CI enbart linting och formatting via ruff. Det
innebär att en regressionsbugg i `bronze_to_silver.py` inte fångas av CI utan
av att fel data dyker upp i Grafana, vilket är alldeles för sent.

- [ ] Skriv om `test_transforms.py` för PySpark (använd en lokal SparkSession
      med `master("local[1]")` i testmiljön, snabb och utan Docker-beroende)

- [ ] Prioriterade tester att täcka: deduplicering på `event_id`,
      null-filtrering på nyckelkolumner, korrekt extrahering av `pr_action`
      och `pr_number` via `get_json_object`, nollpadding i partitionsnycklar

- [ ] Återinför `pytest` i `.github/workflows/ci.yml`

- [ ] Mål: minst 10 meningsfulla tester som täcker transformationslogikens
      kritiska paths

### Dokumentationsskulden

- [ ] Uppdatera `serving.mmd` med Grafana-lagret och MotherDuck-kopplingen

---

## MVP v4 - Datakvalitet och Pipeline Monitoring

Syfte: Ge pipelinen åsikter om sin egen data. Idag producerar systemet
resultat utan att veta om de är korrekta. MVP v4 lägger till ett
kvalitetslager som aktivt blockerar nedströmsjobb(Downstream jobs) om Silver-datan inte
håller måttet, skillnaden mellan ett system som tyst kan producera fel
och ett system som *vet om* att det producerar fel.

Konceptet är **data contracts**: explicita, maskinläsbara löften om vad
datan ska innehålla. Om löftet bryts failar Airflow-tasken synligt istället
för att låta dålig data flöda vidare till Gold.

### Airflow DAG-förändringen

Det nya flödet blir:
```
bronze_to_silver --> quality_check_silver --> silver_to_gold_dbt
```

`quality_check_silver` är en ny Airflow-task som kör Soda Core mot
Silver Parquet-filerna och returnerar ett pass/fail. Om den failar
stoppas DAGen och `silver_to_gold_dbt` körs aldrig.

### Kvalitetskontrakt att implementera

Volymkontroller (Completeness + rimlighet):
- [ ] Antal Silver-rader efter transformation ska vara ≥ 80% av
      antal laddade Bronze-rader (fångar onormalt stor datförlust)

- [ ] Antal nya Silver-rader per körning ska vara > 0 om nya
      Bronze-filer fanns att processa

Null-kontroller på nyckelkolumner (Completeness):
- [ ] `event_id` - aldrig null
- [ ] `event_type` - aldrig null
- [ ] `repo_name` - aldrig null
- [ ] `created_at` - aldrig null

Logiska konsistenskontroller (Validity + Consistency):
- [ ] `pr_cycle_time_hours` ska aldrig vara negativ
      (en PR kan inte stängas innan den öppnas)

- [ ] Om `event_type = 'PullRequestEvent'` ska `pr_action` inte vara null

- [ ] `created_at` ska ligga inom rimligt tidsintervall
      (inte i framtiden, inte före 2020)

### Verktyg: Soda Core
Soda Core använder YAML-baserade kontraktsdefinitioner som är lättlästa
och naturligt kompatibla med hur dbt redan tänker kring tester. Det passar
projektets filosofi om läsbarhet och motiverade val.

- [ ] Lägg till `soda-core-duckdb` (eller `soda-core-spark`) i `pyproject.toml`
- [ ] Skapa `quality/checks/silver_checks.yml` med kontrakten ovan
- [ ] Skapa `quality/run_checks.py` som anropas av Airflow-tasken
- [ ] Integrera som ny task i `github_lake_dag.py`
- [ ] Logga check-resultat till en fil under `data/quality_reports/`
      för historisk spårbarhet

---

## MVP v5 - Djupare Analys och Bredare Insikter

Syfte: Nu när pipelinen är tillförlitlig och datakvaliteten är garanterad
är det dags att ställa mer intressanta frågor. MVP v5 expanderar Silver-schemat
med klassificerade attribut och bygger nya Gold-modeller för insikter som
inte finns i nuvarande pipeline.

### Bot-klassificering som Silver-kolumn

Observationen från EDA: många `actor_login`-värden innehåller "bot" i
namnet, men ett namnbaserat filter är bara den enklaste heuristiken.
MVP v5 lägger till `is_bot BOOLEAN` som en explicit Silver-kolumn med
dokumenterade klassificeringsregler.

**Varför i Silver och inte i Gold?** Klassificeringen är en egenskaps hos
*aktören*, inte hos en specifik aggregering. Samma `is_bot`-logik ska kunna
användas av alla Gold-modeller utan att duplicera logiken - det är DRY
på datanivå.

Klassificeringskriterier att definiera och motivera (steg 1: namnbaserat):
- [ ] `actor_login` innehåller `[bot]`, `-bot`, `_bot` (GitHub Apps-mönster)
- [ ] `actor_login` börjar med `dependabot`, `renovate`, `github-actions`
- [ ] Dokumentera kriterierna explicit i `ARCHITECTURE_CONTEXT.md`
      *innan* koden skrivs

Möjliga steg 2-kriterier (tidsbeteende-baserat, MVP v5+):
- Exakt regelbundna tidsintervall mellan events (< 5 sekunders variation)
- Extrem event-volym per timme jämfört med percentil-fördelningen

### Nya Gold-modeller

- [ ] `bot_vs_human_activity.sql` - andel events från bots vs humans per
      event-typ, per vecka. Svarar på: "Hur stor del av DE-aktiviteten på
      GitHub är automatiserad?"

- [ ] `repo_health.sql` - kombinerar PR-cykeltider, commit-frekvens och
      star-tillväxt till ett sammansatt hälsomått per repo

- [ ] Expandera `tool_growth.sql` med bredare `DE_KEYWORDS` - se över
      `config.py` och lägg till verktyg som saknats (t.ex. Polars, DLT,
      Iceberg, Trino)

### Bredare search-params i config.py

- [ ] Granska nuvarande `DE_KEYWORDS` och identifiera uppenbara luckor
- [ ] Sätt upp ett enkelt A/B-test: kör en bootstrap-körning med utökade
      keywords och mät precision vs recall manuellt på ett sample

---

## MVP v6 - Cloud och RAG

Syfte: Ta ett produktionsmognat, analystätt system och dels flytta det
till cloud-infrastruktur, dels lägga ett intelligent frågegränssnitt
ovanpå Gold-lagret.

*Notera: MVP v6 är avsiktligt löst specificerad - detaljer planeras när
MVP v5 är levererat och kursen "Big Data and Cloud" har gett mer kontext
kring cloud-valen.*

### Cloud-migrering (synkat med kursen Big Data and Cloud)

Kurskrav att uppfylla (F15, F16):
- Skapa en komplett big-data lösning hos en cloud-leverantör
- Analysera och motivera lagringsalternativ, nivåer och kostnader

Preliminär riktning:
- [] Utvärdera Azure Data Lake Storage Gen2 som ersättning för lokal Parquet-storage motivera beslutet med hänsyn till kostnad, prestanda och driftsmiljö (direkt VG-material)

- [] Undersök self-hosting som alternativ (Hetzner + MinIO som S3-kompatibelt lager) - relevant för europeisk datasovereignty-diskussionen.

- [] Dokumentera trade-offs explicit: lock-in, kostnad, latency, compliance

### RAG-lager - "Fråga min data-lake"

Inspirerat av Databricks Genie och min egna erfarenhet från Glossary DB RAG:

Konceptet är en Text-to-SQL pipeline: naturligt språk -> SQL-query mot DuckDB Gold-tabeller -> svar formulerat i naturligt språk. Det är inte magi, det är en LLM som fått rätt kontext om ditt schema och mina tabellers semantik.

- []  Definiera vilka frågor systemet ska kunna svara på (scope: en styr kvaliteten på systemprompten)
- [] Bygg ett enkelt FastAPI-lager ovanpå DuckDB (återanvänder mönster från Glossary DB)
- [] Integrera med befintlig Grafana-dashboard eller bygg ett enkelt chat-gränssnitt

---

## Tidslinje (ungefärlig)

| Fas | Innehåll | Ungefärlig tidshorisont |
|---|---|---|
| Cleanup Sprint | Tech-debt från v3, tagg v3.0.1 | Närmaste 1-2 sessioner |
| MVP v4 | Datakvalitet + monitoring | Sommaren 2026 |
| MVP v5 | Djupare analys + bot-klassificering | Sommaren 2026 |
| MVP v6 | Cloud + RAG | Höst 2026, synkat med kurser + LIA sökande |

---

## Öppna frågor att besvara under resans gång

Dessa är inte blockers - de är frågor att hålla i bakhuvudet och besvaras när rätt kontext finns:

Kan projektet användas som ett långt pågående examensarbete? Kräver mer planering i dialog med STI.

Vilken cloud-leverantör ger bäst balans mellan kostnad, läroupplevelse och europeisk datasovereignty - Azure, GCP, eller self-hosted MinIO?

Hur djupt ska RAG-lagret vara? Enbart Text-to-SQL, eller ska det även inkludera semantisk sökning över commit-meddelanden och PR-beskrivningar?
