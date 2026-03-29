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