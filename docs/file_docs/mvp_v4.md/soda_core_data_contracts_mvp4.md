# own docs regarding soda core, data contracts and quality controls in pipelines.

## Vad är Soda Core?
`Soda Core` är ett open source `CLI`-tool och `python` lib som fokuserar på Datakvalitet(Data Quality). Jag skriver mina tester och regler i enkla och rena `YAML`-filer och `Soda` översätter dem till optimerade `SQL`-queries som körs mot min DB eller mitt Data Warehouse.

- Jag kan t.ex kolla saker som:
    * Finns det `Nulls` i en column som *måste ha data?*
    * Är datan "färsk"(freshness) eller har laddningen stannat?
    * Ligger försäljningssiffrorna inom ett "rimligt" intervall?
    * Har mitt schema ändrats? (**SCHEMA DRIFT** t.ex - en kolumn har bytt namn eller försvunnit) 

--- 

### Vilken huvudvärk som soda core löser för mig.
Först och främst **SILENT FAILURES**. Min `Airflow` DAGs visar att alla tasks är gröna i min graf. Pipen kördes klart, allt är frid och fröjd. *MEN* egentligen så laddade mitt källsystem upp tomma filer, eller så ändrades ett datumformat etc. Datan i min Databas har nu blivit korrupt och när jag väl är tillbaka och ska göra något så kommer `BI`-teamet / `DA`-teamet / `DS`-teamet / `Stakeholders` dra felaktiga slutsatser och beslut pga felaktig data(big nono, big bad.)

- `Soda` fungerar som en "gatekeeper" eller dörrvakt i mitt `quality_check_silver` steg i min pipeline. Så mitt nya `DAGs`-flöde kommer gå från: 
    - `bronze_to_silver -> silver_to_gold_dbt` till 
    - `bronze_to_silver -> quality_check_silver -> silver_to_gold_dbt`

- Om datan från `Bronze` inte riktigt håller måttet enligt mina `YAML` "regler" så stoppar `Soda` pipen redan INNAN *dbt* bygger mina `gold`-modeller/marts. På så vis underviker jag "GIGO" (Garbage in, garbage out) och slösar inte onödig compute på att transformera dålig data.


### En viktig avvägning: Soda vs. dbt tests

Eftersom nästa steg är `silver_to_gold_dbt` har jag nu märkt att dbt också har inbyggda tester (typ, `not_null`, `unique`).

**Varför används båda?**

* **Soda:** Används *innan* transformationen. Fångar raw data/Silver data som är trasig så jag slipper köra hela dbt-projektet i onödan.

* **dbt tests:** Används *efter* transformationen för att dubbelkolla att min egen dbt kod (SQL-logiken) inte skapade duplicates eller bröt någon affärslogik.

Att bygga `bronze_to_silver -> quality_check_silver (Soda) -> silver_to_gold_dbt` är ett steg som är viktigt att fokusera på och förstå, nu när flödet från bronze(raw)-silver(clean)-gold(enriched) är implementerad är det kritiskt att säkerställa att granska datan och säkerställa att datan håller en viss kvalitet.

---

## Skillnad mellan SCHEMA DRIFT(Vad soda core kollar) VS Slowly Changing Dimensions(SCD, det hanterar jag i DBT)
Som rubriken säger är `Schema drift` och `Slowly changing Dimensions` något helt annorlunda.

- Det ena handlar om att **strukturen** på databasen ändras.  

- Det andra handlar om att själva **innehållet** , dvs den faktiska datan ändras över tid.

### 1) Schema Drift (Vad Soda Core kollar)

Schema drift (eller Schema Evolution) handlar om förändringar i datans **struktur** eller metadata i källsystemet.

- **Vad det är:** Utvecklarna av källsystemet (t.ex, en frontend-apps eller ett externt API) ändrar plötsligt hur tabellen är uppbyggd.

- **Exempel:** De byter namn på kolumnen `user_id` till `customer_id`, de tar bort kolumnen `phone_number` helt, eller de ändrar datatypen på `age` från en `INT` till en `STRING`.

- **Problemet:** Om det här sker i "smyg" (vilket det ofta gör), kommer min pipeline att krascha *stenhårt* när den försöker läsa eller transformera data som inte längre ser ut som den förväntar sig.

- **Lösningen i min pipeline:** Det här är `Soda Cores` jobb i mitt `quality_check_silver`-steg. Soda varnar och säger *"STOP, kolumnen 'user_id' saknas i dagens laddning. Hit men inte vidare!"* och pausar flödet innan trasig data når dbt.

### 2. Slowly Changing Dimensions / SCD (Detta hanterar man i dbt)

SCD handlar om hur du modellerar och sparar historik kring datans **innehåll**. Det har ingenting med korrupt data eller trasiga pipelines att göra, utan är rent **datamodelleringsarbete**.

* **Vad det är:** Tabellens struktur (schemat) är exakt likadant som igår, men verkligheten har förändrats.

* **Exempel:** En kund flyttar från Stockholm till Göteborg, eller byter efternamn. Kolumnerna `city` och `last_name` finns kvar och är intakta, men själva *värdet* på raden för den specifika kunden har ändrats.

* **Problemet:** Vill affärssidan (analytikerna) kunna se vad kunden hette *innan* de bytte namn, eller bryr de sig bara om vad kunden heter just nu?

* **Lösningen i min pipeline:** Det här bygger jag i mitt `silver_to_gold_dbt`-steg. Om Jag bygger en **SCD Type 2**, skapar dbt en ny rad för kunden med det nya namnet och sätter `valid_from` och `valid_to`-datum på raderna så att jag får en historik. Om jag bygger en **SCD Type 1** skriver dbt bara över det gamla värdet.

---

* **Soda Core** funkar som ett larm för `**Schema Drift**` (någon har gjort om i source-systemet)

* **dbt** bygger logiken för `**SCD**` (hur man sparar historiken när verkligheten förändras)
---