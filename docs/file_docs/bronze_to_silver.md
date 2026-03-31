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