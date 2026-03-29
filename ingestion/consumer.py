# kafka consumer.py - For ingestion
# Kommentarer: Svenska
# Kod: Engelska
import json
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaError, KafkaException
from loguru import logger

# Importera från min config.py
from config import (
    BRONZE_DIR,
    DATE_PARTITION_FORMAT,
    KAFKA_BROKER,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC_RAW,
    LOG_LEVEL,
)

# --- Logging ---
logger.remove()
logger.add(
    sink=lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
)


# --- Konstanter ---
# Batch storleken styr hur ofta jag skriver till disk. 100x events ELLER 60 sec. Det som inträffar först triggar skrivning.
# För liten batch = för många små Parquet-filer (ineffektivt för PySpark senare).
# För stor batch = för länge mellan skrivningar (mer data att förlora vid krasch).
BATCH_SIZE = 100
BATCH_TIMEOUT_SEC = 60


# --- Parquet skrivning ---
# Privat funktion. DONT TOUCH!
def _write_batch_to_bronze(batch: list[dict]) -> None:
    """
    Writes a batch of events to Bronze layer as Parquet.

    Each event gets its own row. The path is built from the event's created_at
    so that the data ends up in the correct year/month/day partition automatically.

    Grouping the batch by date before writing, a batch can contain
    events from midnight onwards, e.g. 23:59 and 00:01, which go to
    different day folders.
    """
    # Gruppera events per partition datum
    partitions: dict[str, list[dict]] = {}

    for event in batch:
        # Githubs created_at format "Y/M/D time Z"
        created_at = event.get("created_at", "")
        try:
            dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            # Om timestamp saknas eller är ogiltigt, använd UTC nu
            dt = datetime.now(timezone.utc)
            logger.warning(
                f"Invalid created_at for event {event.get('id')} using current UTC time"
            )

        partition_key = DATE_PARTITION_FORMAT.format(
            year=dt.year,
            month=dt.month,
            day=dt.day,
        )
        partitions.setdefault(partition_key, []).append(event)

    # Skriv varje partition till sin egen mapp
    for partition_key, events in partitions.items():
        output_path = BRONZE_DIR / partition_key
        output_path.mkdir(parents=True, exist_ok=True)

        # Konvertera listan av dicts till en PyArrow-tabell.
        # PyArrow ordnar schema automatiskt från datan, jag behöver inte
        # definiera kolumner manuellt i Bronze. Rådata sparas som den är.
        table = pa.Table.from_pylist(events)

        # Bygg ett unikt filnamn baserat på timestamp så att jag aldrig skriver över.
        # Om consumer kör om (efter krasch) skapar den en ny fil bredvid den gamla.
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
        output_file = output_path / f"part-{timestamp}.parquet"

        pq.write_table(table, output_file, compression="snappy")
        logger.info(f"Wrote {len(events)} events -> {output_file}")


# --- Consumer ---
def run_consumer() -> None:
    """
    Main loop. Consumes events from Kafka and writes them to Bronze.

    Flow:
    poll() from Kafka -> collect in batch -> batch full/timeout -> write Parquet -> commit offset

    Note the order: write to disk FIRST, commit to Kafka THEN.
    If script crash after writing but before committing, I rerun and write
    the same data again, that's harmless. If its committed first and then crashed
    I would lose the data permanently.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": KAFKA_GROUP_ID,
            # "earliest" betyder: om det inte finns någon sparad offset för den här
            # consumer group, börja från det äldsta meddelandet i topicen.
            # "latest" hade betytt: börja från det senaste och missa allt historiskt.
            "auto.offset.reset": "earliest",
            # Hanterar commits manuellt (efter diskskrivning)
            # auto.commit=true hade committat på tid, oavsett om det skrivit till disk.
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([KAFKA_TOPIC_RAW])
    logger.info(f"Consumer started | Broker: {KAFKA_BROKER} | Topic: {KAFKA_TOPIC_RAW}")

    batch: list[dict] = []
    last_flush = datetime.now(timezone.utc)

    try:
        while True:
            # poll(1.0) = vänta max 1 sekund på ett nytt meddelande.
            # Returnerar None om timeout nås utan meddelande.
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Ingen data kom in, kontrollera om timeout-flush behövs
                pass
            elif msg.error():
                # PARTITION_EOF är inte ett riktigt fel, det betyder bara
                # att jag nått slutet av en partition just nu. Nya meddelanden
                # kan fortfarande komma in.
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(
                        f"End of partition reached: {msg.topic()} [{msg.partition()}]"
                    )
                else:
                    if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        # Topic existerar inte än, producer har ej skickat något
                        # Vänta lite och försök igen istället för att krascha.
                        logger.warning("Topic not available yet, retrying in 5 sec..")
                        time.sleep(5)
                    else:
                        raise KafkaException(msg.error())
            else:
                # Deserializera JSON string tillbaka till en dict
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    batch.append(event)
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    logger.error(f"Failed to deserialize message: {e}")

            # --- Flush-logik: skriv batch om den är full ELLER timeout nådd ---
            now = datetime.now(timezone.utc)
            elapsed = (now - last_flush).total_seconds()
            batch_full = len(batch) >= BATCH_SIZE
            timeout_reached = elapsed >= BATCH_TIMEOUT_SEC and len(batch) > 0

            if batch_full or timeout_reached:
                reason = "batch_full" if batch_full else "timeout"
                logger.info(f"Flushing batch | reason={reason} | size={len(batch)}")

                # STEG 1: Skriv till disk (kritiskt steg!!)
                _write_batch_to_bronze(batch)

                # STEG 2: Commit offset till Kafka (först när data är säker)
                consumer.commit(asynchronous=False)

                batch.clear()
                last_flush = now

    except KeyboardInterrupt:
        # Ctrl+C, skriv det som finns kvar i batchen innan den stänger
        logger.info("Shutdown signal received")
        if batch:
            logger.info(f"Flushing remaining {len(batch)} events before exit")
            _write_batch_to_bronze(batch)
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()
        logger.info("Consumer closed cleanly")


# --- Entrypoint ---
if __name__ == "__main__":
    run_consumer()
