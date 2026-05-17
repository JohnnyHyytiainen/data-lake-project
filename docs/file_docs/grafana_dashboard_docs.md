# Own docs for Grafana in my serving layer, and its purpose.
Egna docs kring Grafana dashboard jag tänker använda som dashboard i mitt serving layer.

---
## Vad är Grafana och hur skiljer sig det ifrån de dashboards jag använt mig utav innan?(Evidence, Streamlit och PowerBI)

`Streamlit` är en bra referenspunkt här. Bygga en `streamlit`-dashboard är som att bygga någonting från grunden. Tänk ett hus: Jag gjuter grunden, som huset ska byggas på, bygger väggarna, golvet, taket, etc etc. JAG själv bestämmer exakt hur varje element ska byggas och hur det ska se ut och skriver allting i kod. Det är flexibelt och kraftfullt MEN allt arbete landar på mig som programmerare(byggare av huset).

`Grafana` är som att hyra eller köpa ett färdigt hus som någon annan har byggt. Jag behöver inte bygga grunden, designa ritningen med en arkitekt eller på egen hand, de finns redan där. **Mitt** jobb med `Grafana` är att bestämma **VAD** som ska synas och **VARTIFRÅN** färgen på fasaden eller väggarna ska komma ifrån.

    - Enkel förklaring: Jag behöver ej tänka på arkitekturen bakom dashboarden, data loader, vad som är cached etc. Jag behöver enbart välja VAD för data som ska visas och vartifrån datan kommer. `Grafana` hanterar all rendering, responsivitet, tidsfilter, zoom interactions och export funktioner per automatik.

Den dashboard som liknar `Grafana` mest som jag har använt mig utav i tidigare kurs(SQL) är just `Evidence-Dashboard`. Både `Grafana` & `Evidence` är SQL first(Om jag förstått det rätt. Behöver fortfarande läsa på mer och fylla i luckorna här). MEN `Evidence` är en mer.. Dokument orienterad dashboard då en skriver allt i `.md`(markdown) filer. I alla markdown filer skriver jag mina SQL-queries/SQL-block. `Grafana` är mer dashboard orienterat med ett visuellt interface för att konfigurera paneler.

Om jag ska jämföra `Streamlit` VS `Grafana` ännu mer så är `Grafana` ett mer *opinionerat verktyg* (opinionated tool eller opinionated software. Vilket innebär att verktyget har en **tydlig uppfattning** om hur ett problem ska lösas) medan `Streamlit` är mycket friare. `Grafana` är och verkar vara industry standard när det kommer till att visa metrics i realtid.

---

## Den tekniska utmaningen jag nu står framför:

Den uppförsbacken jag står framför nu är att `Grafana` ej stödjer `DuckDB`.. `Grafana` stödjer väldigt mycket internt, så som t.ex `PostgreSQL`, `MySQL`, `Prometheus`, `InfluxDB` natively men *INTE* `DuckDB`.. Dock så finns det ett community made plugin vid namn `marcusolsson-duckdb-datasource` som jag ska installera i min `grafana container` i `Docker`. Liknande issue jag hade med `dbt` och `ghcr.io`, ibland behöver man installera det som inte följer med inbyggt. Förhoppningsvis så visar det sig att det flyter på smidigt annars behöver jag tänka om och kanske gå över till `PostgreSQL` istället för `DuckDB` för golden layer.


### En till sak att ha med i minnet: DuckDB är single-writer.

Att `DuckDB` är Single-writer innebär så som jag förstått det(behöver läsa på mycket mer) att OM jag kör `Airflow` och `dbt` steget som skriver till `github_lake.duckdb` filen exakt samtidigt som `Grafana` försöker läsa från `github_lake.duckdb` så kommer det bli konflikter. Det *BÖR* ej vara några issues då det `Airflow` egentligen endast **SKA** köras vid schemalagda tider.

Men värt att ha med och se över när allting lirar: I ett produktionsystem skulle man lösa det med en read replica eller genom att exportera Gold till ett read-optimerat format om jag förstått det rätt(även här behöver jag fylla på MER information och skriva ner för att verkligen förstå skillnader, fördelar/nackdelar)

## Issues med Grafana container i min Compose-stack
Issue nr 1 vid `docker-compose up grafana -d` är detta:
```
Error: ✗ invalid service state: Failed, expected: Terminated, failure: invalid service state: Failed, expected: Running, failure: not healthy, 0 terminated, 1 failed: [starting module plugin.backgroundinstaller: invalid service state: Failed, expected: Running, failure: failed to install plugin marcusolsson-duckdb-datasource@: 404: Plugin not found]
```

Anledning: 

`marcusolsson-duckdb-datasource` existerar inte längre i `grafanas` officiella plugin registry. Det var ett plugin byggt av Marcus Olsson som ett community projekt men DuckDB plugin är nu byggt och underhålls av `MotherDuck`, företaget bakom cloud baserade `DuckDB`-tjänsten. Plugin'et heter nu istället `motherduck-duckdb-datasource`. Liknande issue jag stötte på när jag skulle få dbt att lira (Se `Dockerfile.dbt` och `ghcr.io`).

# Hur grafana dashboard byggs upp och JSON strukturen kring att bygga dashboarden.
Vad JSON strukturen faktiskt gör och de tre viktigaste koncept i `de_community.json`-filen som är viktiga att förstå och träna mer på: 

- Varje panel har en `gridPos` som definierar position och storlek i Grafanas 24-kolumners rutnät. Panel 1 är 24 kolumner bred och tar hela bredden. Panel 2 och 3 är 12 kolumner vardera och sitter bredvid varandra på rad två. Det är hela layouten, inga pixlar, bara rutnätskoordinater.  

- `targets` är arrayen av SQL queries som panelen kör. En panel kan ha flera queries (A, B, C...) och kombinera resultaten. Än så länge har mina tre paneler endast en query var.

- `transformations` på den första panelen är viktig att förstå. Min SQL returnerar data i long format, dvs tre kolumner som är: `time`, `repo_name`, `cumulative_stars`. En rad, per repo, per vecka. Grafanas `timeseries-panel` förväntar sig wide format. Alltså en kolumn per repo. `prepareTimeSeries`-transformationen konverterar automatiskt från long till wide, så varje repo blir en egen linje utan att jag behöver bråka med att pivota i SQL.


---
## Grafana dashboarden är up 'n' running.
Dashboarden fungerar och går att nå via localhost. 1/3 grafer makes sense. 
- `DE Tool Growth` - Räknar cumulative stars över tid
- `Community Activity heatmap - hours X weekday` - Ska räkna antal events när DE communityt är aktivt på github.
- `Pr Cycle Time - Median & P95` - Ska räkna hur lång tid en typisk PR cykel tar.

För tillfället så är det enbart `Tool growth` grafen som ser förståbar ut. Första jobbet för mig just nu är att fixa till `activity_heatmap` grafen och sen lösa `pr cycle times` grafen. Detta då `Pr cycle` problemet är ett `SQL`-issue i grunden där jag enbart kan filtrera bort outliers eller justera skalan. Min `Activity_heatmap` är däremot en annan fråga, en fråga i hur just `Grafana` tolkar data.

Konceptuell utmaning att förstå: Min `Activity_heatmap` tabell returnerar 3x kolumner, dessa är: `day_of_week`(Ett tal på 0-6/1-7), `hour_of_day`(0-23) och `event_count`(antal github events dag X och timme Y). Problemet jag står för nu är nog att `Grafana` inte vet hur den ska tolka `day_of_week` och `hour_of_day`.


Just nu så ser min tabell för heatmappen ut så här:
```
day_of_week | hour_of_day | event_count
0           | 0           | 423
0           | 1           | 387
0           | 2           | 290
...
6           | 23          | 891
```

^ Tabellen ser ut så här för att min query returnerar datan i long format, det här är "long format" om jag uppfattat det rätt. Varje rad är en observation med tre kolumner. `Grafana` förväntar sig dock något i *WIDE FORMAT* och inte long format. Varje dag bör vara sin egen kolumn och varje rad bör representera en timme på dygnet. Ungefär så här:

```
hour_of_day | Mån   | Tis   | Ons   | 
0           | 423   | 387   | 290   | 
1           | 312   | 445   | 501   | 
```

För att jag ska kunna omvandla från Long till Wide behöver jag göra en `pivot-operation`. Det går att ordna på två sätt. En med `DuckDBs` inbyggda `PIVOT` syntax eller med en transformation i `grafana`-ui't. Jag bör dock göra det med SQL då en dashboard ej ska stå för några tunga lyft öht.

Lösningen bör vara detta:
```sql
PIVOT (
    SELECT
        hour_of_day,
        -- Gör dagarna läsbara istället för 0-6
        CASE day_of_week
            WHEN 0 THEN '1_Mon'
            WHEN 1 THEN '2_Tue'
            WHEN 2 THEN '3_Wed'
            WHEN 3 THEN '4_Thu'
            WHEN 4 THEN '5_Fri'
            WHEN 5 THEN '6_Sat'
            WHEN 6 THEN '7_Sun'
        END AS weekday,
        event_count
    FROM activity_heatmap
)
ON weekday
USING SUM(event_count)
GROUP BY hour_of_day
ORDER BY hour_of_day ASC
```

Att använda `1_`, `2_` prefix framför dagarna är en liten men viktig detalj. Jag vill ordna alla veckodagar så som vi människor läser dom, alltså från Måndag->Onsdag->Söndag annars blir det krångligt och onödigt jobbigt att behöva organisera allting i minnet. Med prefixet 1...2...6...7 före så blir det mycket enklare att se ordningen och följa flödet hela veckan.

Lösningen ovan var naiv, det returnerade felkod: `sql: Scan error on column index 1, name "2_Tue": unsupported Scan, storing driver.Value type *big.Int into type *string: Could not process SQL results` i `Grafana`. En annan lösning behövs.

Efter att ha googlat runt och försökt hitta lösningar och rådfrågat C kring lösning så föreslog C att jag wrappar min `PIVOT` i en `subquery`. Med förklaringen att låta `PIVOT` göra sitt jobb inne i en `subquery` och sen i det yttre lagret `CAST`a varje kolumn till en type som GO-drivrutinerna förstår. Samma "princip" som ett medallion layer i miniatyr. Dvs, varje lager har **ett** specifikt ansvar och `type converting` är det yttre lagrets ansvar. Queryn bör se ut något liknande denna:

```sql
SELECT
    hour_of_day,
    -- Castar varje dag-kolumn från HUGEINT till INTEGER
    -- INTEGER (32-bit) bör räcka för event-räkningen per timme
    CAST("1_Mon" AS INTEGER) AS "1_Mon",
    CAST("2_Tue" AS INTEGER) AS "2_Tue",
    CAST("3_Wed" AS INTEGER) AS "3_Wed",
    CAST("4_Thu" AS INTEGER) AS "4_Thu",
    CAST("5_Fri" AS INTEGER) AS "5_Fri",
    CAST("6_Sat" AS INTEGER) AS "6_Sat",
    CAST("7_Sun" AS INTEGER) AS "7_Sun"
FROM (
    -- Inner query: PIVOT gör long till wide omvandlingen
    PIVOT (
        SELECT
            hour_of_day,
            CASE day_of_week
                WHEN 0 THEN '1_Mon'
                WHEN 1 THEN '2_Tue'
                WHEN 2 THEN '3_Wed'
                WHEN 3 THEN '4_Thu'
                WHEN 4 THEN '5_Fri'
                WHEN 5 THEN '6_Sat'
                WHEN 6 THEN '7_Sun'
            END AS weekday,
            event_count
        FROM activity_heatmap
    )
    ON weekday
    USING SUM(event_count)
    GROUP BY hour_of_day
)
ORDER BY hour_of_day ASC
```
Vilket ej fungerade heller. Anledning: `error querying the database: Binder Error: Column "1_Mon" referenced that exists in the SELECT clause - but this column cannot be referenced before it is defined`-felkod.

`DuckDB` verkar inte tillåta att `PIVOT` resultatets columns wrappas i ett yttre `SELECT`-statement. Efter lite mer research och frustration stötte jag på något som kallas för: `conditional aggregation`, ett SQL pattern som vad jag förstått det gör nästan exakt samma sak som `PIVOT` men utan att förlita sig för `DuckDB`-specifik dialekt. 

**Konceptet bakom conditional aggregation** så som jag förstått det är detta:
Grundtanken är "enkel". Istället för att låta databasen automatiskt skapa columns från rad värden så skapar jag varje kolumn manuellt med en `CASE WHEN`-sats inne i en `aggregerings funktion`. Dvs, Varje kolumn säger i princip "Summera `event_count` men bara för de rader där `day_of_week` är det här *specifika* värdet, annars räkna noll. Det är en `Pivot`-operation uttryckt i standard `SQL` och bör se ut så här:

```sql
SELECT
    hour_of_day,
    -- Varje dag blir en egen column via CASE WHEN + SUM
    -- CAST direkt här, ingen subquery behövs.
    CAST(SUM(CASE WHEN day_of_week = 0 THEN event_count ELSE 0 END) AS INTEGER) AS "Sunday",
    CAST(SUM(CASE WHEN day_of_week = 1 THEN event_count ELSE 0 END) AS INTEGER) AS "Monday",
    CAST(SUM(CASE WHEN day_of_week = 2 THEN event_count ELSE 0 END) AS INTEGER) AS "Tuesday",
    CAST(SUM(CASE WHEN day_of_week = 3 THEN event_count ELSE 0 END) AS INTEGER) AS "Wednesday",
    CAST(SUM(CASE WHEN day_of_week = 4 THEN event_count ELSE 0 END) AS INTEGER) AS "Thursday",
    CAST(SUM(CASE WHEN day_of_week = 5 THEN event_count ELSE 0 END) AS INTEGER) AS "Friday",
    CAST(SUM(CASE WHEN day_of_week = 6 THEN event_count ELSE 0 END) AS INTEGER) AS "Saturday"
FROM activity_heatmap
GROUP BY hour_of_day
ORDER BY hour_of_day ASC
```

NOTERA: Ordningen på dagarna i Queryn beror på SPARK vs ISO standard. Spark följer Javas kalenderstandard där `dayofweek()` returnerar 1=sunday, 2=monday... ..., veckan börjar på Söndagen vilket är den amerikanska konventionen. Det är förklarat i mitt `silver_to_gold.py` script. Dock så bör jag cirkulera tillbaka till detta och fixa det, det faktum att jag behövde gå tillbaka till ett script för att komma ihåg sparks interna dagkonvention för att kunna tolka min `Grafana`-graf korrekt är ett tecken på att den konventionen *egentligen* borde ha normaliserats redan i silver eller i gold steget... En väldesignad Pipeline bör inte exponera interna representations detaljer ifrån ett verktyg(`spark` i mitt fall) till konsumenterna längre ner i kedjan. I framtida iterationer av `build_activity_heatmap` hade det varit cleant att lägga till en column som normaliserar till ISO standard DIREKT i PySpark.