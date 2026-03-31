# Own docs for bronze_to_silver.py script.

Vidare till `bronze_to_silver.py` Innan jag skriver en rad kod, ska jag förstå vad Silver-lagret faktiskt är vs vad Bronze är. För skillnaden är fundamental och det är **kritiskt** att jag vet skillnaden.

## Bronze är min storage och SSOT
```
Bronze är som ett råmagasin, jag kastar in precis vad GitHub skickar utan att jag ifrågasätter något. Det är avsiktligt. Om jag hade ett fel i min valideringslogik och filtrerade bort viktig data i Bronze finns det inget att gå tillbaka till. 

Bronze ÄR min försäkring att ingenting försvinner som jag vill använda mig av. Här samlar jag bara på mig all data. Mer data = bättre.
```
## Silver är där min data slutar vara "saker jag fått ner" och börjar vara "Data jag kan lita på"

Silver är där min data slutar vara "data jag fick" och börjar vara "data att lita på." Det är tre konkreta transformationer som händer i ordning. 

1) Först validering - Här kontrollerar jag att varje event faktiskt har de fält jag förväntar mig: id, type, actor, repo, created_at. Saknas ett kritiskt fält går eventet till en Dead Letter Queue istället för att tyst förstöra min data längre fram i pipelinen. 

2) Sedan deduplicering - GitHub Events API kan returnera samma event vid flera poll-cykler om det är nytt nog. Jag droppar dubbletter baserat på event_id. 

3) Slutligen flattenning - ett råevent från GitHub ser ut ungefär så här:

```json
{
  "id": "12345",
  "type": "PushEvent",
  "actor": {"login": "johnnyhyy"},
  "repo": {"name": "apache/airflow"},
  "payload": {"commits": [{"sha": "abc123"}]},
  "created_at": "2026-03-29T16:00:00Z"
}
```

3) forts: Den nästade strukturen med actor.login och repo.name är bra för JSON men jobbig för analys. Silver-lagret plattar ut det till rena kolumner: actor_login, repo_name, commit_count. Något som PySpark och dbt kan jobba med utan att behöva gräva i nästade strukturer.

**NOTERA:**
- En viktig sak för min MVP v1: Här använder jag mig av Pandas och **inte** PySpark. PySpark kommer i MVP v2. Pandas räcker perfekt för de volymer jag har nu och är enklare att debugga. När jag väl har hela min Silver logik klar och testad är det ett "relativt" litet steg att porta det till PySpark, då logiken bör förblir densamma, bara syntaxen byter skepnad.


## De tre viktiga designbeslut i bronze_to_silver.py som är kritiska att förstå på djupet:

1) Det första är att Bronze aldrig ska röras. `run_bronze_to_silver()` läser från `BRONZE_DIR` och skriver till `SILVER_DIR` och `DLQ_DIR`. Det är en enkelriktad väg. Om Jag om sex månader inser att min `_is_valid()` funktion hade en bugg och felaktigt skickade bra events till DLQ, inga problem, Bronze har dem fortfarande och jag kan köra om transformationen med fixad kod. Det är den fundamentala filosofiska skillnaden jämfört med Dataplatform Dev kursens DLQ i `consumern`.

2) Det andra är varför jag deduplicerar i Silver och inte i Bronze. I Bronze vill jag ha en exakt avspegling av vad som faktiskt kom in i systemet, dubbletter och allt. Om producern av någon anledning skickade ett event två gånger till Kafka är det ett faktum som ska dokumenteras i Bronze. Silver är däremot platsen där en förbereder data för analys, och i en analys vill du aldrig att samma commit räknas två gånger i ett aktivitetsdiagram.

3) Det tredje är att `_flatten()` är medvetet selektiv. Jag plockar inte ut allt från raw eventet, bara det som faktiskt behövs för att svara på Gold-lagrets frågor. `pr_action` och `pr_merged` är valfria och sätts till None för event-typer där de inte existerar, vilket är fullt acceptabelt i Silver. Gold-lagret filtrerar sedan på event_type för att arbeta med rätt delmängd.


## Viktig information att ha med sig.

Efter att ha kört `bootstrap_historical.py` scriptet och hämtat data ifrån Github Archives stötte jag på problem med att få min hämtade data från Bronze -> silver.

- Felet var detta:

```
$ uv run python -m transforms.bronze_to_silver
2026-03-31 20:41:59 | INFO | Starting Bronze -> Silver Transformation
2026-03-31 20:41:59 | INFO | Found 98 parquet files in Bronze
2026-03-31 20:42:00 | INFO | Loaded 10461 total events from Bronze layer
2026-03-31 20:42:01 | INFO | Validation complete | valid=10461 invalid=0
2026-03-31 20:42:01 | INFO | Removed 18 duplicate events
Traceback (most recent call last):
  File "<frozen runpy>", line 198, in _run_module_as_main
  File "<frozen runpy>", line 88, in _run_code
  File "C:\Users\johnn\Desktop\projekt\data-lake-project\transforms\bronze_to_silver.py", line 210, in <module>
    run_bronze_to_silver()
  File "C:\Users\johnn\Desktop\projekt\data-lake-project\transforms\bronze_to_silver.py", line 186, in run_bronze_to_silver
    [_flatten(row.to_dict()) for _, row in df_valid.iterrows()]
     ^^^^^^^^^^^^^^^^^^^^^^^
  File "FILEPATH......projekt\data-lake-project\transforms\bronze_to_silver.py", line 100, in _flatten
    .get("merged", False),
     ^^^
AttributeError: 'NoneType' object has no attribute 'get'
```
- Lösningen på problemet var denna rad i `bronze_to_silver.py`

Felet:
```python
event.get("payload", {}).get("pull_request", {}).get("merged", False)
```
Lösningen:
```python
"pr_merged": (event.get("payload", {}).get("pull_request") or {}).get("merged", False),
```

- Anledningen till varför `or {}` löste ett jobbigt problem är detta:

```
Logiken vid första blick ser korrekt ut. Om pull_request saknas, använd {} som default MEN det finns ett fall jag ej räknade med.
Github kan skicka "pull_requests": null i sin JSON..

När pandas läser det från .parquet konverteras null till pythons None och None är ej samma sak som {en tom dict}. None är ingenting och ingenting har ingen .get() method.

Skillnaden är som ett öppet men tomt rum, {} är ett tomt rum, jag kan gå in och leta efter saker i rummet iallafall.

None är en stängd dörr och jag kan inte ens gå in i rummet OCH om jag ens försöker så kraschar jag in i den.

Fixen var ett litet 'or {}' som säger att, om resultatet är None, behandla det som en tom dict istället.
Med 'or {}' skyddar jag mig för just det scenario jag just stötte på. Om .get("pull_request") returnerar None byter jag ut det mot {} innan jag försöker anropa .get("merged", False). Är den metaforiska dörren stängd? Byt ut den mot ett öppet men tomt rum istället och fortsätt.
```

## Ännu en bug upptäcktes.

Output:
```
2026-03-31 21:16:20 | INFO | Wrote 58032 Silver records -> FILEPATH\projekt\data-lake-project\data\silver\events\year=2026\month=03\day=24\part-20260331_191620_956401.parquet
2026-03-31 21:16:20 | INFO | Bronze -> Silver transformation completed.
```

- Bug som gömde sig i koden och ej var förväntad då jag enbart fått fåtal events per poll och dagarna ej har varit någon anledning att oroa mig för bugs men efter hämtning av data ifrån github archive skrevs all data till day=24 foldern i silver.

Anledningen: fanns i `_write_parquet()` i `bronze_to_silver.py` scriptet. `.iloc[0]`tar den första radens `created_at` och använde den som en REPRESENTATIV partition för hela min DataFrame. Det fungerade som sagt bra när jag enbart hade data ifrån en enda dag, men nu med data ifrån 7 dagar gick det ej bra. Pandas råkade sortera den så att den äldsta raden hamnade FÖRST, därav `day=24` för ALLTING.

Det är exakt samma bug som idempotens problemet jag stötte på och löste tidigare, fast i en annan form. Lösningen är att gruppera DataFramen per datum **INNAN** jag skriver, precis som jag gjorde i consumer. 

- Lösningen var att: Ersättaa *hela _write_parquet() anropet i slutet av `run_bronze_to_silver()` med följande logik:*

```python
# Gruppera df_silver per dag och se till att skriva  varje grupp till rätt partition.
# På så sätt hamnar events från 2026-03-24 i day=24 och events från
# 2026-03-25 i day=25 oavsett i vilken ordning pandas har sorterat dem.
df_silver["_date"] = pd.to_datetime(
    df_silver["created_at"]
).dt.date

for date, group in df_silver.groupby("_date"):
    # Ta bort hjälpkolumnen innan jag skriver, den hör tydligen inte hemma i Silver(Proof -> Buggen jag upplevde nyss)
    group = group.drop(columns=["_date"])
    _write_parquet(group, SILVER_DIR, label="Silver")

logger.info("Bronze -> Silver transformation completed.")
```

- Lösningen: var även att i `_write_parquet()`-funktionen behövde jag justera hur partitionssökvägen byggdes eftersom att jag nu skickar in en grupp där alla rader har samma datum.

```python

def _write_parquet(df: pd.DataFrame, output_dir: Path, label: str) -> None:
    """
    Writes a DataFrame to Parquet with same Hive-style partitioning
    that Brone layer uses. This way Pyspark can read Silver in the exact
    same way as Bronze. Consistent structure throughout the lake.
    """
    if df.empty:
        logger.info(f"No {label} records to write, skipping.")
        return

    # Parsa datum från created_at för att bygga korrekt partition
    # Använder första radens datum som representant för hela batchen
    sample_dt = datetime.fromisoformat(df["created_at"].iloc[0].replace("Z", "+00:00"))
    partition = (
        f"year={sample_dt.year}/month={sample_dt.month:02d}/day={sample_dt.day:02d}"
    )
    output_path = output_dir / partition
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    output_file = output_path / f"part-{timestamp}.parquet"

    df.to_parquet(output_file, index=False, compression="snappy")
    logger.info(f"Wrote {len(df)} {label} records -> {output_file}")
```