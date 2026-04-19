# Own docs for marts and tool_growth.sql script


## Vad scriptet gör
Istället för att skapa en VIEW så som `stg_github_events` scriptet gör så börjar jag nu skapa riktiga tables.  
`ref()` är dbt's sätt att deklarera beroenden. dbt vet nu att `stg_github_events` måste existera innan den här modellen kan köras. `date_trunc('week')` avrundar `created_at` till Måndagen i samma vecka. Alla events som sker under samma vecka får på så vis samma KEY, vilket gör processen att gruppera och jämöra veckor mycket enklare.

Kumulativt(Samlad ökning) av antal stars som ett repo får över tid är något jag mäter. `sum() OVER` är en window function. Den summerar `star_count` för **ALLA** rader med samma `repo_name` som är sorterade i tidsordning. Se det som en "running total", dvs varje vecka läggs på föregående vecka.

## Teori kring window functions vs group by i tool_growth
`GROUP BY` i all ära, men för just mitt projekt och mitt syfte hade en vanlig `GROUP BY` varit ganska så destruktivt. Destruktivt i den mening att `GROUP BY` komprimerar MÅNGA rader till EN. Om jag grupperar på `repo_name` så försvinner helt plötsligt all viktig information om enskilda veckor och jag får endast **EN** rad per repo... `window functions` för något helt annat, den **behåller** alla rader men tillåter varje rad att "titta ut" och se sina "grannar"(andra rader). 

Varje rad i `weekly` CTE har sin egen `week_start` och `star_count`, men `cumulative_stars` beräknas genom att summera alla tidigare rader med samma `repo_name` i tidsordning.
Exempel:
Vecka 13, Måndag 5 stars, Tisdag 1 star, Onsdag 0 stars, Torsdag 10 stars, Fredag 4 stars, Lördag 3 stars, Söndag 0 stars. == 23 stars vecka 13.