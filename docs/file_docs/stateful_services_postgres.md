# Document regarding stateful services and postgres issue
Misstag ifrån mitt håll: `git clean -fdx`.

Efter en tänkt "städning" och brist på information och kunskap råkade jag tidigare rensa allt som är .gitignored, vilket var ett stort misstag. Efter den rensningen och uppbyggnaden igen får jag nu issues med min postgres db i docker containern. Felkod: `[1951] FATAL:  role "airflow" does not exist`

---
Felet här är att jag skapade min postgres volume med ett visst username och password, vid min "accidental nuke" så ändrades min `.env`, det vill säga annat username och password. Vilket nu har lett till att `.env` filen och min postgres volume har glidit isär utan att jag har märkt av det förens nu. 

Det är ett state som lever UTANFÖR min kod och har sin alldeles egna livscykel.

## The fix:
Fixen för att lösa problemet en gång för alla är:

1) - `docker compose down` - Detta stoppar mina containers, INGEN -v då jag inte vill röra mina volumes
2) - `docker volume rm data-lake-project_postgres_data` - För att REMOVE min nuvarande postgres volume i containern.
3) - `docker compose up -d postgres` - Upp med ny postgres volume igen, ENBART postgres.
4) - `docker compose logs postgres` - Här ska jag leta efter 'skipping initialization' som **INTE** ska finnas med nu.

Hur vet jag då att problemet är löst? Jo, problemet är löst om jag inte ser 'Skipping initialization' och istället ser en normal initdb-output, dvs skapandet av databas och roller.

Efter jag vet att loggen **INTE** visar "skipping initialization" kan jag gå vidare till:

5) - `docker compose up airflow-init` - Ska nu köra migrate + skapa admin user utan fatal error.
6) - `docker compose up -d` - Starta upp resten av stacken

