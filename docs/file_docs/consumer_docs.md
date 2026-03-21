# Own docs for producer.py script
Nästa naturliga steg efter producern är consumern. Den som *LYSSNAR* på kafka topicen och skriver alla events till `Bronze` som Parquet. Consumern är den **andra** halvan av ingestion lagret och när den väl är på plats så är `Bronze` klar. Då är bronze pipelinen klar. Från `Github API -> Producer -> Kafka -> Consumer -> Parquet`

## Snabb teoretisk överblick om consumern.
```
Consumern har ett mycket viktigare och ännu mer komplext jobb än producern. Producern är s.k 'stateless'. Den bryr sig inte om vad som hände tidigare poll cykler. Consumern däremot måste tänka på: 

- BATCHING, dvs samla ihop events och skriva dom som en Parquet fil istället för att skriva en fil per event.

- Offset hantering, Kafka håller koll på vad consumern redan har läst så att OM den kraschar kan den fortsätta där den slutade.

- Partitionering, Skriva till rätt year=/month=/day/ folder automatiskt baserat på eventets timestamp.
```
