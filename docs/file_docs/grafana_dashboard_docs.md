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

