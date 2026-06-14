# Quality/checks/silver_checks.yml
Docs for basic `Soda CL syntax`

`checks for <namn>:` är root noden. namn måste matcha exakt det VIEW-namn jag skapar i DuckDB i nästa steg. Om VIEW'en heter silver_events och checks-blocket heter silver_events_check hittar Soda ingenting.

- Check-typerna du ser nedan:

- `missing_count(kolumn) = 0` - räknar NULL-värden. missing = null eller tom sträng.

- `duplicate_count(kolumn) = 0` - räknar icke-unika värden. Bra för primary keys.

- `invalid_count(kolumn) = 0` med `valid min: 0` - räknar värden utanför det angivna intervallet. 
    - **Kritisk skillnad:*  `invalid_count` hoppar över `null`-värden. Det betyder att `pr_cycle_time_hours` som är `null` för icke-merged PRs inte triggar denna check, jag kontrollerar bara de rader där värdet faktiskt existerar och säger att det inte får vara negativt. Exakt den semantiken som jag vill ha.

- `failed rows:` med `fail condition:` - det mest kraftfulla verktyget. Soda genererar SQL med `WHERE fail_condition` och failar om någon rad returneras. Används för konditionella kontroller som inte passar de fördefinierade check-typerna.