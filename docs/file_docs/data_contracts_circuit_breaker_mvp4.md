# MVP v4: Data Quality & Circuit Breaker – Incident & Design Docs

## 1. Koncept & Arkitektur ("The Why" - Varför jag byggde det här)

Målet med MVP v4 var att gå från en pipeline som bara kan flytta data, till en pipeline som aktivt övervakar och skyddar mitt Gold layer från korrupt raw data.

* **Data Contracts:** Ett maskinläsbart avtal/"kontrakt" (i det här fallet, en `YAML`-fil via `Soda Core`) som definierar exakt hur datan i Silver-lagret *måste* se ut.

* **Circuit Breaker (Dörrvakten):** En `Airflow`-task (`quality_check_silver`) som körs *mellan* Silver och Gold lagren. Bryts mitt data contract stängs pipelinen ner (`sys.exit(1)`), `Airflow` "larmar" rött, och `dbt` körs aldrig. Det här är ett sätt att förhindra "Garbage in, Garbage out"(GIGO).

* **Schema Drift vs. SCD:**
* *Schema Drift (Soda/Silver):* Innebär strukturella förändringar i källan(source) (t.ex en `column` försvinner eller byter `datatype`). Det kraschar min pipeline och fångas av `Soda` vid daglig schemalagd körning.

* *Slowly Changing Dimensions / SCD (dbt/Gold):* Innebär att 'Verkligheten' förändras (t.ex en `user` byter förnamn eller användarnamn) men `tabellstrukturen` är densamma. Det här är ren `data modeling` och ska hanteras i `dbt`.

---

## 2. Incidentlogg & Lösningar (Vad som gick fel(Silent failure) och hur det löstes)

### Fälla 1: "Fail Open" (Tysta fel/Silent failures)

* **Symptom:** `Airflow` DAG'en lyste grön och Soda rapporterade "SUCCESS", trots att `YAML`-filen hade ett syntaxfel (`data_source:` istället för `data_source silver_duckdb:`).

* **Root Cause:** `Python`-scriptet utvärderade noll tester men returnerade standard felkod `0` (vilket betyder att allt gick bra). "Dörrvakten"(`circuit breaker`) var lat och sov på jobbet men loggade att allting gick bra i pipen från `silver-layer` -> `gold-layer`.

* **Lösningen:** `Python`-logiken uppdaterades med `has_error_logs()`, om `Soda` misslyckas med att läsa konfigurationen eller `datasourcen` tvingas scriptet krascha med `sys.exit(1)`.

`Python`-logiken uppdaterades med `has_error_logs()`, om `Soda` misslyckas med att läsa konfigurationen eller `datasourcen` tvingas scriptet krascha med `sys.exit(1)`.
---
### Fälla 2: Type Mismatch (String vs Timestamp)

* **Symptom:** Soda kraschade med `Binder Error: Cannot compare values of type VARCHAR and type TIMESTAMP WITH TIME ZONE`.

* **Root Cause:** `JSON`-payloads från Github har ingen inbyggd `timestamp`-datatype. `PySpark` sparade `created_at` som en `STRING` i mina .parquet filer. När Soda försökte jämföra datan och fick datatypen `string` med funktionen `now()` (som är en riktig `timestamp`) kraschade DuckDB, vilket är helt rätt.

* **Lösning:** *Två åtgärder* 
1) `TRY_CAST` i `Soda`-kontraktet som defensiv patch. 
2) `F.to_timestamp()` i `bronze_to_silver.py`, framtida Silver-filer skrivs med korrekt `TIMESTAMP`-typ. Cold run kördes för att rensa gamla `VARCHAR`-filer.

---

### Fälla 3: The Missing Column (Affärslogik i fel lager)

* **Symptom:** `Binder Error: Referenced column "pr_cycle_time_hours" not found in FROM clause`.

* **Root Cause:** Ett `Soda`-kontrakt letade efter negativa PR-cykeltider i Silver-lagret. Men den kolumnen (och logik) existerar inte förrän i `dbt` (Gold-layer).

* **Lösningen:** Insikten att `Soda` (Silver) *bara* ska validera *struktur och raw data*, medan `dbt` (Gold) validerar *affärslogik*. Kontrollen raderades från `YAML`-filen.

---

### Fälla 4: The 10,000 Limit Crash (Okända Github Actions)

* **Symptom:** Soda rapporterade `FAIL` med varningen att fler än 10 000 rader bröt mot ett kontrakt, men ingen specifik breakdown visades i loggarna.

* **Root Cause (Data Modeling Bug):** `PySpark`-scriptet döpte raw datas `payload.action` till `pr_action` för *alla* events. `Soda`-kontraktet krävde att `pr_action` bara fick innehålla `PR`-specifika värden (`'opened'`, `'merged'` etc). Det gjorde att över 300 000 helt legitima `WatchEvents` (med action `'started'`) och `IssueCommentEvents` (med action `'created'`) flaggades som korrupta.

* **Lösningen:** 
1. Ett diagnostic script (`diagnose_soda.py`) byggdes för att ställa direkta `SQL queries` till `DuckDB` för att isolera felet och försöka lösa felet.

2. `Soda`-kontraktet skrevs om till en "villkorlig" logik: Kontrollera *bara* `pr_action` OCH om `event_type = 'PullRequestEvent'`.

3. `samples limit: 50` läggdes till i alla `YAML` checks för att förhindra `OOM`-issues (minneskrascher).

---

## 3. Best Practices & Viktiga Insikter att ta med sig

* **Diagnostik slår gissningar:** När ett verktyg (som Soda) brister i sin felrapportering, bygg ett mindre, isolerat script som ställer frågor direkt till din raw data (som med DuckDB och `diagnose_soda.py`).

* **Docker-hygien:** Om koden ändras lokalt i t.ex `VScode`, måste en ny `Docker image` byggas (`docker build...`) innan `Airflow` kan exekvera den nya logiken.

* **Allowlist vs Blocklist:** Att bara blockera all data man inte känner igen så kallat en `Blocklist` kan strypa downstream `modeller` som `tool_growth.sql` som i mitt fall kräver `WatchEvents`. Att släppa igenom datan men skydda `domain logic` med specifika villkor (s.k `Allowlist` för specifika `event-typer`) är ofta säkrare när källan är ett externt API.

* **Hantera Technical Debt:** Det är helt okej att ha felnamngivna kolumner (som att `payload.action` heter `pr_action` för alla events) så länge det är ett medvetet val, tydligt dokumenterat, och schemalagt för en framtida refaktorering (MVP v5).

---