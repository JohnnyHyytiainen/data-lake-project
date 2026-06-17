# Docs regarding deeper EDA and insights/findings regarding Github Data

### Schema Silver + Bronze:
- Antal kolumner i Bronze: `10`
- Antal kolumner i Silver: `13`

- Oväntade datatyper i Bronze (t.ex VARCHAR istället för TIMESTAMP): `actor` och `repo` innehåller STRUCT som är intressanta, även `public` som innehåller `BOOL` för private/public.
    - `actor` i Bronze: `STRUCT(avatar_url VARCHAR, display_login VARCHAR, gravatar_id VARCHAR, id BIGINT, login VARCHAR,...)`
    - `repo` i Bronze: `STRUCT(id BIGINT, "name" VARCHAR, url VARCHAR)`

- Oväntade datatyper i Silver (t.ex VARCHAR istället för TIMESTAMP): `BIGINT` för year? Exempel: 2026, 2025, 2027, 2039, bör räcka med `INTEGER`. Likadant med `event_id`, Är nu `VARCHAR` men kanske bör göras till `BIGINT`? Eventuellt om en OBT ska modelleras med dimensioner(Star Schema) kanske det bör hashas eller användning av `DENSE_RANK()`?

### Volym & Täckning
- Totalt antal Silver-rader: `2 400 535`
- Datumintervall täckt: `2025/07/01 -> 2026/06/17`
- Månader/dagar med luckor: `Alla månader har täckning, lägst har 2026/06 med 2.8% av event_count vilket är självklart. Spridningen av datan per månad i event_count är:`

- Fördelning av events per månad i %  

|year|month|event_count|percent_of_total|
|:---|:---|:---|:---|
|2025|	07|	220109	|9.2  |
|2025|	08|	279900	|11.7 |
|2025|	09|	226933	|9.5  |
|2025|	10|	195386	|8.1  |
|2025|	11|	204626	|8.5  |
|2025|	12|	208504	|8.7  |
|2026|	01|	205294	|8.6  |
|2026|	02|	199992	|8.3  |
|2026|	03|	231751	|9.7  |
|2026|	04|	184984	|7.7  |
|2026|	05|	175695	|7.3  |
|2026|	06|	67179	|2.8  |

### Event Types
- Dominerande event-typ och andel: `PushEvent` dominerar med 64.65% av allt, lägst är `ForkEvent` med 1.41% kompletta listan ser ut så här:

| event_type | event_count | percent_of_total |
|:---|:---|:---|
| PushEvent | 1551885 | 64.65|
| CreateEvent | 303639 | 12.65 |
| PullRequestEvent | 296454 | 12.35 |
| IssueCommentEvent	| 132992 | 5.54 |
| WatchEvent | 81526 | 3.40 |
| ForkEvent | 33857 | 1.41 |



- Andel PullRequestEvent (input till pr_cycle_times): `296 454` vilket är 12.35%

- Bör jag justera DE_KEYWORDS-filtret i MVP v5? `Ja`, har förslag kring hur listan ska utökas.


### pr_action Kontaminering
- Andel kontaminerade rader: `9.48%`, sammanfattningen ser ut så här:

| total_rows | pr_event_rows | contaminated_rows | contamination_percent |
|:---|:---|:---|:---|
| 2400353 | 296454.0 | 227476.0 | 9.48 |



- Bedömning: kosmetiskt / analytisk risk: `Kan vara analytisk risk, nästan 9.5% i datan, bör grävas djupare i och förstå varför.`

- Prioritet på rename `pr_action` --> `event_action`: hög / medel / låg: `medel`


### Bots
- Uppskattad andel bot-events (namnbaserat): `21.58%`

- Räcker namnbaserad klassificering för MVP v5? `Osäker ännu`

- Namnmönster att addera utöver `[bot]`, `dependabot`, `renovate`: `-bot`, `_bot`

### Kända Begränsningar Verifierade
- `pr_merged` alltid False: Bekräftat ✓ / Ej bekräftat ✗: `Ej bekräftat.`

- `pr_action = 'merged'` som merge-indikator: Bekräftat ✓ / Ej bekräftat ✗: `Ej bekräftat`

| pr_action | pr_merged | count |
|:---|:---|:---|
| assigned|	False|	4015|
| closed|	False|	12242|
| closed|	True|	39602|
| labeled|	False|	75971|
| merged|	False|	38515|
| opened|	False|	122649|
| opened|	True|	71|
| reopened|	False|	1105|
| unassigned|	False|	22|
| unlabeled|	False|	2262|


### Beslut för MVP v5
- `is_bot`-klassificering: namnbaserad räcker / behöver beteendebaserat tillägg: `Ja, det BÖR räcka.`

- `pr_action` -->  `event_action` rename: approach och timing: `Ja, jag bör döpa om. Absolut. Se findings under`

- Silver-kolumner att lägga till: `Osäker`

- Silver-kolumner att ta bort: `Osäker`

- Nya Gold-modeller att prioritera baserat på EDA: 

    - `Bots VS human pattern` När är bots mest aktiva, tid på dygn, dag? Likadant med människor.

    - `Hype VS Reality index` WatchEvents VS Push/PR. Växer ett framework bara i hype, stars/watchEvents eller växer det i faktiskt utveckling? Marten: en dbt model som per vecka och repo mäter ratio mellan WatchEvent(Stars) och faktiska bidrag(PushEvent + PullRequestEvent), kan visualiseras som kanske en scatterplot? Om ett repo har 5k stjärnor men 2 pushes på en månad är det en varningsklocka för att ett open source projekt kanske är döende och folk bara har det i sin starred lista som när man glömmer att ta bort gamla appar i telefonen.

    - `Bots vs human automation ratio` Hur stor andel av DE communityts aktivitet är "rent underhåll". Marten, när jag bygger `is_bot` kan jag skapa en modell som mäter `is_bot = True` vs `False` per repo? En stacked barchart i serving layer? 

    - `Comunity health och contributor churn` Hålls projekten vid liv av EN ensam person eller är det ett stort community? Marten, här kan jag räkna `COUNT(DISTINCT actor_login)` per repo och filtrera bort de identifierade botsen med `WHERE NOT is_bot`? Serving layer, en graf som visar unika mänskliga contributors över tid`Om ett repo växer i användning men tappar unika code contributors, vad innebär det? Pekar det möt ökat tech debt?

    - `Time to first review / label churn`, Hur SVÅRT är det egentligen att få in sin kod i olika open source projekt på github? Marten, här kan man kanske använda de `75 000` `Labeled`-händelserna jag hittade för `PRs`. Hur många labels och kommentarer för en genomsnittlig `PR` innan den stängs och mergas? Serving layer, en dashboard panel för open source contributors - "Vilket DE repo har snabbast feedback loop?"

------

# Resultat av EDA:

## Sektion 1 - Silver + bronze schema inspection:

**Describe för BRONZE**
- Antal columns i bronze är: 10


|column_name | column_type | null | key | default | extra|
|:---|:---|:---|:---|:---|:---|
|id|	VARCHAR|	YES|	None|	None|	None|
|type|	VARCHAR|	YES|	None|	None|	None|
|actor|	STRUCT(avatar_url VARCHAR, display_login VARCHAR, gravatar_id VARCHAR, id BIGINT, login VARCHAR,...|	YES|	None|	None|	None|
|repo|	STRUCT(id BIGINT, "name" VARCHAR, url VARCHAR)|	YES|	None|	None|	None|
|payload|	VARCHAR|	YES|	None|	None|	None|
|public|	BOOLEAN|	YES|	None|	None|	None|
|created_at|	VARCHAR|	YES|	None|	None|	None|
|day|	VARCHAR|	YES|	None|	None|	None|
|month|	VARCHAR|	YES|	None|	None|	None|
|year|	BIGINT|	YES|	None|	None|	None|

---
**Describe för SILVER**
- Antal columns i silver är: 13

| column_name | column_type | null | key | default | extra |
|:---|:---|:---|:---|:---|:---|
| event_id	|VARCHAR|	YES|	None|	None|	None|
| event_type|	VARCHAR|	YES|	None|	None|	None|
| actor_login|	VARCHAR|	YES|	None|	None|	None|
| repo_name|	VARCHAR|	YES|	None|	None|	None|
| repo_id|	VARCHAR|	YES|	None|	None|	None|
| commit_count|	INTEGER|	YES|	None|	None|	None|
| pr_number|	INTEGER|	YES|	None|	None|	None|
| pr_action|	VARCHAR|	YES|	None|	None|	None|
| pr_merged|	BOOLEAN|	YES|	None|	None|	None|
| created_at|	TIMESTAMP|	YES|	None|	None|	None|
| day|	VARCHAR|	YES|	None|	None|	None|
| month|	VARCHAR	|YES	|None	|None	|None|
| year|	BIGINT|	YES|	None|	None|	None|

---

**Hur ser silver datan ut?**
| event_id | event_type | actor_login |	repo_name |	repo_id | commit_count | pr_number|	pr_action| pr_merged | created_at | day| month | year |
|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|:---|
| 51530481835 | PushEvent|github-actions[bot]|RedHatInsights/ubi-trino|319443023	|1	|0	|NaN	|False	|2025-07-01 00:43:44	|01	|07 |2025|
|51530511158|	IssueCommentEvent|	dependabot[bot]|	mbhavya/prefect|	394979719|	0|	0|	created|	False|	2025-07-01 00:45:05|	01|	07|	2025|
|51531580186|	ForkEvent|	wcnmnnd|	LibreSpark/LibreTV|	961357433|	0|	0|	NaN|	False|	2025-07-01 01:33:01|	01|	07|	2025|
|51530556046|	CreateEvent|	Jagadishchandra17|	Jagadishchandra17/-End-to-End-Data-Engineering-Pipeline-for-Uber-NYC-Taxi-Data|	1011524741|	0|	0|	NaN|	False|	2025-07-01 00:47:09	|01	|07	|2025|
|51532679396|	PullRequestEvent|	dependabot[bot]|	pmdevers/MinimalKafka.Dashboard|	994610761|	0|	5|	opened|	False|	2025-07-01 02:20:44	01|	07|	2025|

---

## Sektion 2 - Volym och density, hur mycket data och NÄR

**Total radräkning + volym per månad.**

- Totalt antal silver rows är: 2,400,353 rader

|year|month|event_count|percent_of_total|
|:---|:---|:---|:---|
|2025|	07|	220109	|9.2  |
|2025|	08|	279900	|11.7  |
|2025|	09|	226933	|9.5  |
|2025|	10|	195386	|8.1  |
|2025|	11|	204626	|8.5  |
|2025|	12|	208504	|8.7  |
|2026|	01|	205294	|8.6  |
|2026|	02|	199992	|8.3  |
|2026|	03|	231751	|9.7  |
|2026|	04|	184984	|7.7  |
|2026|	05|	175695	|7.3  |
|2026|	06|	67179	|2.8  |

---

- Fördelning av events PER dag:


|min_events_per_day	|p25	|median	|p75	|p95|	max_per_day|	days_with_data|
|:---|:---|:---|:---|:---|:---|:---|
|20	|6022|	6900|	7583|	8981|	44982|	348|

---

- Dagar med MINST data för att hitta potentiella ingestion luckor. Se efter om det är vissa dagar som har ett gap där jag inte hämtat data(missat någon dag) eller om det bara är "långsamma" dagar:

| year| month|	day|events_per_day|
|:---|:---|:---|:---|
| 2026| 06|	16|	20|
| 2025| 10|	11|	26|
| 2025| 10|	12|	29|
| 2025| 10|	13|	31|
| 2026| 06|	14|	35|
| 2025| 10|	09|	49|
| 2025| 10|	10|	56|
| 2025| 10|	14|	1022|
| 2026| 04|	24|	2301|
| 2026| 04|	23|	2357|
| 2026| 05|	09|	2577|
| 2026| 05|	07|	2663|

---

## Sektion 3 - Distribution av EVENT TYPES:
- `Github Events` API producerar ~30 olika `event-types`. Jag filtrerar på `DE_KEYWORDS` i min `producer`, men **VILKA** `event-typer` dominerar i Silver lagret?
    - Varför det spelar roll för MVP v5?

    - **pr_cycle_times** baseras *enbart* på `PullRequestEvent`, hur liten/stor andel är det?

    - **activity_heatmap** inkluderar alla event-typer - bör jag vikta dem?

    - **is_bot** - bots dominerar i vissa event-typer mer än andra (t.ex `IssueCommentEvent`)

    - **DE_KEYWORDS-filtret** - fångar jag upp rätt saker, eller för bred/smal? <-- **SVAR:** För smalt IMO

---

- Event type distribution, hur ser allting ut i % i min nuvarande data

| event_type | event_count | percent_of_total |
|:---|:---|:---|
| PushEvent | 1551885 | 64.65|
| CreateEvent | 303639 | 12.65 |
| PullRequestEvent | 296454 | 12.35 |
| IssueCommentEvent	| 132992 | 5.54 |
| WatchEvent | 81526 | 3.40 |
| ForkEvent | 33857 | 1.41 |

---
**MOTBEVISAD(?)**

- PullRequestEvents: Vad är fördelningen i pr_actions? Merge PRs har pr_action = 'merged' INTE pr_action = 'closed' + pr_merged = True (github bootstrap quirk och kanske github api quirk)

| pr_action | count | percent |
|:---|:---|:---|
| opened | 122720 | 41.40 |
| labeled | 75971 |	25.63 |
| closed | 51844 | 17.49 |
| merged | 38515 | 12.99 |
| assigned | 4015 | 1.35 |
| unlabeled | 2262 | 0.76 |
| reopened | 1105 | 0.37 |
| unassigned | 22 | 0.01 |

---

## Sektion 4 - pr_action contamination audit
**Känt problem:**
`get_json_object(payload, '$.action')` extraheras från *alla* event-typer till kolumnen `pr_action`.
`WatchEvent` har `action: 'started'`, `ForkEvent` har `action: 'forked'`.
Kolumnen *heter* `pr_action` men innehåller egentligen `event_action`.

Planerat fix i MVP v5: `pr_action` ska göras om till `event_action`.

**Frågorna att besvara idag:**
1. Hur stor % av alla rader har icke-null `pr_action` fast det INTE är `PullRequestEvent`?
2. Vilka action-värden kontaminerar kolumnen?
3. Kosmetiskt problem eller analytisk risk?

Svaret avgör *prioriteten* på rename beslutet i MVP v5.

---

- Icke pr events med icke null `pr_actions`, pr_action per event_type exkluderar PullRequestEvent


| event_type | pr_action | count |
|:---|:---|:---|
| IssueCommentEvent | created |	132992 |
| WatchEvent | started | 81526 |
| ForkEvent	| forked | 12958 |


---

- Sammanfattning av total kontaminering av min data i %:

| total_rows | pr_event_rows | contaminated_rows | contamination_percent |
|:---|:---|:---|:---|
| 2400353 | 296454.0 | 227476.0 | 9.48 |


---

## Sektion 5 - Actor och bot pattern analys
- En stor del av aktiviteten på GitHub är automatiserad: `Dependabot`, `Renovate`, `GitHub Actions`, `custom CI-bots`. Om jag analyserar aktivitet utan att filtrera bort `bots` mäter jag `*automationsmönster*`, inte `*developer-beteende*`, det är en analytisk risk för alla mina Gold-modeller.

- Planen i MVP v5 är att lägga till `is_bot BOOLEAN` i mitt Silver schema. Varför Silver och inte Gold?
    - Jo för att klassificeringen är en egenskap hos *aktören* och *inte* hos en specifik *aggregering*. Alla Gold modeller kan använda `WHERE NOT is_bot` utan att behöva duplicera logiken(**DRY**)

- Hör undersöker jag:
1) Hur dominerande är egentligen bot aktiviteten?
2) Vilka namning patterns förekommer?
3) Räcker det med enbart namnbaserad klassificering eller behöver jag klura ut några beteendebaserade heuristiker?

---

- Top 30 actors by event_count:

| actor_login | event_count | unique_event_types | unique_repos |
|:---|:---|:---|:---|
| github-actions[bot]|	145864|	4|	2873|
| dependabot[bot]|	95056|	4|	3623|
| pull[bot]|	93447|	2|	381|
| gemabintang3108-prog|	63706|	1|	1|
| kosteev|	49517|	3|	3|
| sPARks82flick517luxe|	29725|	2|	1|
| dbt-cloud[bot]|	27970|	4|	5241|
| lovable-dev[bot]|	26477|	2|	1497|
| renovate[bot]|	26466|	4|	402|
| atishay-kasliwal|	25919|	2|	2|
| schrockn|	24537|	4|	5|
| MiNIsPARKroar|	20408|	1|	1|
| mapr-devops|	12615|	2|	11|
| biOSPaRkfurYq|	12219|	1|	1|
| Copilot|	11759|	4|	985|
| semaphore-agent-production[bot]|	10916|	3|	25|
| zzstoatzz|	8400|	6|	26|
| novasparkesegql|	8376|	1|	1|
| sparklyballs|	7369|	2|	9|
| potiuk|	6357|	5|	132|
| vercel[bot]|	5438|	4|	617|
| Lewatoto|	5090|	1|	2|
| fa-assistant|	5036|	4|	32|
| boring-cyborg[bot]	|4902|	2|	11|
| aws-airflow-bot	|4878|	4|	1|
| dongjoon-hyun	|4235	|4|	11|
| neutrinoceros	|3931	|5	|125|
| github-merge-queue[bot]|	3890|	2|	41|
| adymob2024|	3643|	1	1|
| Mytherin	|3285	|5	|51|

---

- Bot kategorisering, namnbaserade mönster(räcker det för att kunna identifiera bot-patterns eller behöver jag hitta mer mönster för att identifiera bots säkert?)

| actor_category | event_count | percent_of_total |
|:---|:---|:---| 
| Human (presumed)|	1882065 |	78.41 |
| GitHub App [bot]|	497184|	20.71 |
| Suffix -bot|	13057|	0.54 |
| Suffix _bot|	6278|	0.26 |
| Renovate | 1658| 0.07 |
| Snyk | 111 | 0.00 |

---

- Vilka SPECIFIKA bot logins förekommer mest i datan? Top 35 "identifierade" "bot" logins:

| actor_login | event_count |
|:---|:---|
| github-actions[bot] | 145864 |
| dependabot[bot]|	95056 |
| pull[bot]|	93447 |
| dbt-cloud[bot]|	27970 |
| lovable-dev[bot]|	26477 |
| renovate[bot]|	26466 |
| semaphore-agent-production[bot]|	10916 |
| vercel[bot]|	5438 |
| boring-cyborg[bot]|	4902 |
| aws-airflow-bot|	4878 |
| github-merge-queue[bot]|	3890 |
| mrbro-bot[bot]|	2971 |
| airlock-confluentinc[bot]|	2944 |
| flinkbot|	2739 |
| cursor[bot]|	2694 |
| devin-ai-integration[bot]|	2590 |
| service-bot-app[bot]|	2577 |
| sonarqubecloud[bot]|	2178 |
| codecov[bot]|	2016 |
| coderabbitai[bot] | 1945 |
| regro-cf-autotick-bot|	1712|
| openshift-ci[bot]	|1638|
| codecrafters-publish-to-github[bot]|	1610|
| renovate-bot	|1521|
| cla-bot[bot]	|1502|
| lightspark-bot	|1383|
| shopify[bot]	|1309|
| knative-prow[bot]	|1251|
| hackindia-hakathons[bot]|	1250|
| mergify[bot]	|1108|
| pre-commit-ci[bot]	|1008|
| red-hat-konflux[bot]	|888|
| cloudflare-workers-and-pages[bot]|	835|
| tina-cloud-app[bot]	|834|
| duckdblabs-bot	|822|

---

## Sektion 6 - analys av Nulls per column
- Mina `Soda core`-contracts (MVP v4) testar *specifika* kolumner mot `null`-values. Här ser jag `null`-rate för **ALLA** kolumner.

**Förväntat:**
    - `event_id`, `event_type`, `repo_name`, `created_at` bör vara 0% `NULL` pga mina data contracts i MVP v4.
    - `pr_action`, `pr_number`, `pr_merged` bör ha ganska hög `null`-rate (enbart relevant för `PullRequestEvent)

Något oväntat här == Ny insikt som innebör ett potentiellt nytt `soda core`-`data contract` eller en `schema`-ändring i Silver.

---

- Resultat av `Nulls` per column:
    - totalt 2,400,334 rader:

| column | null_count | null_percent |
|:---|:---|:---|
| pr_action | 1876423.0 | 78.17 |
| event_type	| 0.0	|0.00 |
| actor_login	| 0.0	|0.00 |
| repo_name	| 0.0	|0.00 |
| event_id	| 0.0	|0.00 |
| repo_id	| 0.0	|0.00 |
| commit_count	| 0.0	|0.00 |
| pr_number	| 0.0	|0.00 |
| pr_merged	| 0.0	|0.00 |
| created_at	| 0.0	|0.00 |
| day	| 0.0	|0.00 |
| month	| 0.0	|0.00 |
| year	| 0.0	|0.00 |

---

## Sektion 7 - Bronze, hur ser min raw data ut INNAN transformation?
- Silver är transformerad och validerad men Bronze är orört, det är EXAKT vad github API skickar eller det jag hämtat hem via bootstrap historical scriptet.

**Varför bronze payload är en JSON sträng och inte ett nested object**: PyArrow infererar schema från hela batchen. `PushEvents` har `commits` som lista, `WatchEvent` har det inte alls. `PyArrow` kollapsar inkonsistenta nested structures tyst, `commits` försvinner utan felmeddelanden. Lösningen: `json.dumps()` i consumern.

- Tre kända begränsningar att verifiera *empiriskt:* 
1) `pr_merged` alltid `False` i silver. Beror på vad Bronze Payload faktiskt innehåller
2) `pr_action = 'merged'` som merge indikator. Syns det *tydligt* i Bronze?
3) `payload`-struktur per `event-type`, vad kan jag faktiskt extrahera?
---

- Bronze schema, jämför column name och types med silver. Förväntat: Bronze har mest sannolikt type och inte event_type och payload som VARCHAR:

|column_name|	column_type|	null|	key|	default|	extra|
|:---|:---|:---|:---|:---|:---|
| id|	VARCHAR|	YES|	None|	None|	None|
| type|	VARCHAR|	YES|	None|	None|	None|
| actor|	STRUCT(avatar_url VARCHAR, display_login VARCHAR, gravatar_id VARCHAR, id BIGINT, login VARCHAR,... |	YES|	None|	None|	None|
| repo|	STRUCT(id BIGINT, "name" VARCHAR, url VARCHAR)|	YES|	None |	None |	None |
| payload|	VARCHAR|	YES|	None|	None|	None |
| public|	BOOLEAN|	YES|	None|	None|	None |
| created_at|	VARCHAR|	YES|	None|	None|	None |
| day|	VARCHAR|	YES|	None|	None|	None|
| month|	VARCHAR|	YES|	None|	None|	None|
| year|	BIGINT|	YES|	None|	None|	None| 

---
**DEBUNKED OCH MOTBEVISAD**  
- Är pr_merged alltid `False`? Verifierar en "känd" begränsning om att pr_merged alltid är false och att pr_action = 'merged' är en korrekt merge indikator

| pr_action | pr_merged | count |
|:---|:---|:---|
| assigned|	False|	4015|
| closed|	False|	12242|
| closed|	True|	39602|
| labeled|	False|	75971|
| merged|	False|	38515|
| opened|	False|	122649|
| opened|	True|	71|
| reopened|	False|	1105|
| unassigned|	False|	22|
| unlabeled|	False|	2262|


---

- Första bronze raden per event-type, undersöker payload:

|event_type|	extracted_action|	payload_preview|
|:---|:---|:---|
| CreateEvent|	NaN|	{"ref": "main", "ref_type": "branch", "master_branch": "main", "description": null, "pusher_type...|
| ForkEvent|	NaN|	{"forkee": {"id": 1011512552, "node_id": "R_kgDOPEp06A", "name": "LibreTV", "full_name": "Asch-x...|
| IssueCommentEvent|	created|	{"action": "created", "issue": {"url": "https://api.github.com/repos/apache/spark/issues/51325",...|
| PullRequestEvent|	opened|	{"action": "opened", "number": 42, "pull_request": {"url": "https://api.github.com/repos/weiyila...|
| PushEvent|	NaN|	{"repository_id": 909799682, "push_id": 25215800632, "size": 1, "distinct_size": 1, "ref": "refs...|
| WatchEvent|	started|	{"action": "started"}|

---



## Förslag på utökade search params i config med Repos/topics som pekar mot DE, DS och Data Analytics

```python
DE_KEYWORDS = [
    # Kärnan & Orchestration (använder redan min befintliga bas + tillägg)
    "dbt", "airflow", "spark", "kafka", "flink", "dagster", "prefect", 
    "mage", "kestra", "apache-beam", "luigi",

    # Data Lake, Formats & Table Formats
    "duckdb", "delta-lake", "iceberg", "hudi", "apache-arrow", 
    "parquet", "avro", "protobuf",

    # Data Warehouses & Modern OLAP
    "trino", "clickhouse", "snowflake", "bigquery", "redshift", 
    "starrocks", "druid", "pinot",

    # Data Movement (ELT/ETL)
    "airbyte", "fivetran", "meltano", "dlt", "singer", "kafka-connect",

    # Data Quality, Observability & Contracts
    "soda-core", "great-expectations", "monte-carlo", "datahub", 
    "data-contracts", "amundsen", "data-lineage",

    # Data Science, ML & AI Infra (MLOps)
    "mlflow", "kubeflow", "sagemaker", "jupyter", "pandas", "pyspark", 
    "polars", "scikit-learn", "tensorflow", "pytorch", "huggingface", "dvc",

    # Analys & BI (Headless BI / Semantic layers / Dashboards)
    "superset", "metabase", "redash", "grafana", "tableau", "cube", "lightdash",

    # Streaming & Real-time
    "redpanda", "pulsar", "materialize", "ksqldb", "spark-streaming",

    # Generella branch-termer
    "data-engineering", "data-engineer", "data-science", "data-scientist", 
    "data-analytics", "analytics-engineering", "data-warehouse", 
    "data-lakehouse", "etl", "elt", "mlops"
]
```