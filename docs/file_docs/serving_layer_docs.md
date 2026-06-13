# Own docs for serving layer, and its purpose.

Egna docs kring serving layer.
---
Serving layer eller visningslagret/serveringslager är oftast den sista delen i data-arkitekturen(OFTAST inom lambda-arkitektur(Läs på mer om denna) och `Medallion`-arkitektur/`gold`-layer). Syftet med serving layer är att leverera bearbetad(aggregerad/berikad), strukturerad och affärs ready data till stakeholders/end users, BI-verktyg(Business intelligence) eller ML modeller.

**Serving-lagrets roll i medallion arkitektur**  

*VARFÖR hör Grafana hemma här? Jag har tre lager:*  

* Bronze lagrar raw data.

* Silver transformerar och validerar den datan.

* Gold aggregerar och svarar på business frågor(Eller de frågor en nu vill ta reda på).

    * Men Gold är fortfarande bara filer på disk(lokalt) eller sparade på annan plats(cloud storage etc). Det finns inget värde för en människa med tusentals/miljontals med filer som är svårlästa eller väggar av text, det säger inte så mycket. Värdet i datan existerar inte förens en människa kan **SE** svaren ifrån all data.

* Serving lagret är bryggan mellan Golds aggregerade data och de riktiga insikterna
    * Exempel: En pipeline utan serving är som en bok på ett språk ingen kan läsa: all information finns där, men ingen kan läsa boken eller ens förstå den.

--- 
**Vad är serving lagret?**  

- Det här lagret är oftast optimerat för läsning. Dvs optimerat för snabba queries, pre-aggregated(Pre joined etc. Dvs, det jag gjorde i DBT med datan)  

- Det minskar belastningen och ska ej sköta tunga lyft, det sker i tidigare lager. Se punkt ovan, OPTIMERAT för hastighet.  

---

## Vad Serving.mmd diagrammet visar och vad det betyder(Se även serving_layer_restore.md)
Varför diagrammet är byggt så som det är och vad det inkluderar:

- Varje beslut som kostade mig tid genom att ha råkat skriva `git clean -fdx` och rensa alla untracked filer i min .gitignore ledde till lite kaos. Alla secrets, all data och hela serving-layer försvann. Här är docs kring i varför det tog längre tid att återställa än tänkt:

1) - **Ubuntu vs Alpine** (`Grafana-noden`). `grafana/grafana:latest-ubuntu` i min docker-compose.yml ser ut som en liten detalj MEN det är kritiskt att välja rätt version. `duckdb-go`(`Go`-drivern som det pluginet använder för att prata med DuckDB) är kompilerad mot `glibc`(Linux vanliga C library). `Alpine` använder  `musl libc` istället, en helt annan implementation som är inkompatibel här. 
        - **Resultatet:**  är att det failar i "tystnad". `Grafana` startar, pluginet verkar laddas MEN `Duckdb-connection` fungerar aldrig. Det är en liten men kritiskt detalj för att mitt serving-layer ska kunna visa någonting ifrån alla lager under.

2) - **HUGEINT --> BIGINT (DuckDB noden)** `DuckDB` är designad för analytiska queries(OLAP) och väljer den säkraste datatypen den kan vid aggregering. `SUM()` av en `BIGINT`-column returnerar en `HUGEINT`(128-bit) för att *aldrig* riskera overflow. `Grafanas` `Go`-driver däremot förväntar sig en datatyp av `int64`(64-bit), den kan inte hantera `big.Int`. 
        - **Resultatet:** x antal felmeddelanden jag stötte på.
        - **Fixen:** Alltid *EXPLICIT* `CAST(SUM(x) AS BIGINT)` i alla `Grafana`-queries. Det här är även det dokumenterat direkt i `DuckDB`-noden.

3) - **Deterministiskt UID (Provisioning-noden)** `uid: github-lake-duckdb` i `duckdb.yml`-filen är inte där för att vara *dekorativ*.. Dashboard-`JSON`-filen `de_community.json` refererar till datasourcen via just *det* `UID:t`. **OM** det saknas eller är annorlunda så *kan* `Grafana` *inte* koppla ihop `datasource` med dashboard vid just provisioning. Jag kommer därför se "`datasource not found`" utan någon som helst vidare förklaring. 
        - **Fixen:** Ett *explicit* `UID` gör provisioning deterministisk och upprepningsbar.

4) - **DuckDB `.lock-fil` + volume utan `:ro`** `DuckDB` är ett så kallat `single-writer-system`. Det innebär att även vid *läsning* så skapas det en `.lock`-fil i samma folder som `.duckdb`-filen. Om jag nu väljer att min `data/`-volume mountas som *read-only* så kan `DuckDB` *aldrig* skriva den filen och detta leder som man kan förstå till **krasch**. Detta är varför `data/`-mounten saknar `:ro`(read only) *trots* att `Grafana` tekniskt sett bara *läser data*. Detta är ett undantag som är kritiskt att förstå och ha dokumenterat.