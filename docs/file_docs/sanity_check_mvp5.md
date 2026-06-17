# docs for sanity_pr_merge.py script
Innan jag går in och ändrar i min `pr_cycle_times.sql`-mart är det ganska så kritiskt att jag gör en sanity check för att säkerställa att att resultaten är tomma så att jag kan gå vidare med refaktoriseringen av just `pr_cycle_times`-marten.

- Djupare förklaring:

Om jag ska riva en innervägg i min lägenhet och byta ut den för att ha en öppen planlösning så är det första jag gör inte att ta fram släggan, *först* så *måste* jag kontrollera om väggen jag planerar att riva ner är bärande. Om den nu *är* bärande och jag river utan att byta ut mot en balk som kan bära vikten så måste hela konstruktionen(taket etc) bäras upp av något annat annars rasar allting ner. Sanity checken `sanity_pr_merge.py` är för exakt just den kontrollen. Frågan jag ställer mig är: Finns det `PRs` i *datan* som dyker upp med BÅDA `merge`-typerna för samma `repo_name + pr_number`?  
- **OM JA:** och jag bygger joinen som fångar *båda* så skapar jag `duplicates` i mitt `gold layer`, samma `PR` kommer då räknas två gånger och min `pr_count` blir 'inflated' vilket innebär att `median_hours` kommer bli *skewed*.

- **OM NEJ:** och resultatet är tomt - "väggen" är inte bärande och full fart framåt!

- Sanity check scriptet:

```python
result = duckdb.sql("""
    SELECT
        repo_name,
        pr_number,
        COUNT(*) AS merge_events
    FROM read_parquet('data/silver/events/**/*.parquet', hive_partitioning=true)
    WHERE event_type = 'PullRequestEvent'
      AND (
          pr_action = 'merged'
          OR (pr_action = 'closed' AND pr_merged = True)
      )
    GROUP BY repo_name, pr_number
    HAVING COUNT(*) > 1
    ORDER BY pr_number DESC
    LIMIT 25
""").df()

if result.empty:
    print("Tomt resultat, inga PRs har båda merge-typerna. Säkert att gå vidare.")
else:
    print(f"{len(result)} PRs har BÅDA merge-typerna, granska innan DBT ens rörs!:")
    print(result)
```

- Resultatet av sanitycheck är:

25 PRs har BÅDA merge-typerna, granska innan DBT ens rörs:
| repo_name | pr_number | merge_events |
|:---|:---|:---|
| DS219/spark-seprep | 53 | 2|
| DS219/spark-seprep | 50 | 2|
| DS219/spark-seprep | 39 | 2 |
| florianvazelle/flink | 32 | 2 |
| EreliaStudio/Sparkle | 29 | 2 |
| DS219/spark-seprep | 28 | 2 |
| DS219/spark-seprep | 23 | 2 |
| breezy-bays-labs/cmux-workspace-dbt | 20 | 2 |
| TwentyFifthNight/SparkSales | 16 | 2 |
| cww2697/Spark-Launcher | 12 | 2 |
| TwentyFifthNight/SparkSales | 12 | 2 |
| florianvazelle/flink | 9 | 2 |
| DS219/spark-seprep | 9 | 2 |
| tomtom215/duckdb-behavioral | 8 | 2 |
| DS219/spark-seprep | 8 | 2 |
| davidxchen/languages-sparkling-cf | 6 | 2 |
| davidxchen/languages-sparkling-cf | 5 |2 |
| Priasenthil/advanced-data-engineering-snowflake | 5 | 2 |
| connavy/dbt-test-reviewer | 5 | 2 |
| TwentyFifthNight/SparkSales | 5 | 2 |
| joshcabana/verity-spark-moment-main | 5 | 2 |
| faruksedik/data-engineering-toolkit | 5 | 2 |
| Priasenthil/advanced-data-engineering-snowflake | 4 | 2 |
| furkancftcd/gz-dbt-repository | 4 | 2 |
| SarangSuknale/financial_data_pipeline_dbt_snow... | 4 | 2 |

---

## Resultatet av sanity checken innebär ett problem...
- Det är inte direkt ett `silver-layer` problem utan mer ett `Github`-API beteende. Jag har 70 rows där `PRs` har *BÅDA* merge typerna. När en `PR` mergas via `Github Merge Queue` så skickar `Github` *två separata events för samma `merge-action`*.  

1) `PullRequestEvent` med `action: 'merged'` =  Merge Queue signalen ("Kön är klar")

2) `PullRequestEvent` med `action: 'closed'` + `pull_request.merged: true` = Den vanliga "stängnings signalen".

    - Det är samma `PR`, samma `merge`-ögonblick men två poster i Silver. För de flesta `Merge Queue-PRs` i min data har antingen det ena eller det andra eventet hamnat i pipelinen (kafka timing eller bootstrap-gap). För just dessa **70** `PRs` som Sanity check scriptet identifierade så landade båda två i silver. Det är som att få en bekräftelse och ett kvitto på köpet via mail på att jaghar köpt en produkt samt att jag får kvittot i handen direkt vid köpet. Ett köp - TVÅ kvitton på olika platser. Det funkar IRL kanske men är onödigt och orsakar problem när det kommer till Data.

## Lösningen: `GROUP BY + MIN()`
- Lösningen på det här bör vara användningen av `GROUP BY` + `MIN()` i `closed CTEn`. Istället för att läta den expanderade `WHERE` klausulen producera `N` rows per `PR` så aggregerar jag bort `duplicates` DIREKT i mitt `Common Table Expression(CTE)`.

- `GROUP BY repo_name, pr_number` bör garantera en rad per `PR`. `MIN(created_at)` väljer den första `mergen`

