# kafka_producer.py - For ingestion
# Kommentarer: Svenska
# Kod: Engelska
import json
import time

import requests
from confluent_kafka import Producer
from loguru import logger

# Importera mina keywords och konstanter
from config import (
    DE_KEYWORDS,
    GITHUB_EVENTS_URL,
    GITHUB_TOKEN,
    KAFKA_BROKER,
    KAFKA_TOPIC_RAW,
    LOG_LEVEL,
    POLL_INTERVAL_SEC,
    RELEVANT_EVENT_TYPES,
)

# --- Logging ---
logger.remove()  # Tar bort logurus default handler
logger.add(
    sink=lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
)


# --- Kafka delivery callback ---
# confluent-kafka är async, köar msgs och skickar i bakgrunden.
# Funktionen anropas automatiskt när Kafka bekräftar eller nekar ett msg. Utan den vet jag aldrig om något tappas.
# Privat funktion. DO NOT TOUCH
def _on_delivery(err, msg) -> None:
    if err:
        logger.error(f"Kafka delivery failed. Reason: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")


# --- Github API ---
# Privat funktion. DO NOT TOUCH
def _build_headers() -> dict:
    """Builds request header. Token is optional but gives 5k requests per hour instead of 60."""
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return headers


# --- Github API ---
# Privat funktion. DO NOT TOUCH
def _is_de_relevant(event: dict) -> bool:
    """
    Check if an event is relevant to the DE community.

    Two criteria must both be true:
    1. The event type is in RELEVANT_EVENT_TYPES (PushEvent, PREvent etc.)
    2. The repo name contains a DE keyword (dbt, airflow, spark etc.)

    Why filter already in producer and not in silver?
    The GitHub Events API returns ~300 events per poll. The majority are
    irrelevant (people pushing to their private hello world repos).
    I filter early to avoid cluttering Bronze with data that I
    will never use. Bronze = raw data that I choose to save, not
    everything that exists.
    """
    event_type = event.get("type", "")
    repo_name = event.get("repo", {}).get("name", "").lower()

    if event_type not in RELEVANT_EVENT_TYPES:
        return False

    return any(keyword in repo_name for keyword in DE_KEYWORDS)


# --- Github API ---
def fetch_events(headers: dict) -> list[dict]:
    """
    Retrieves public events from Github API.
    Returns empty list on error so the poll loop never crashes.
    """
    try:
        response = requests.get(GITHUB_EVENTS_URL, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Github API request failed. Reason: {e}")
        return []


# --- Producer ---
def run_producer() -> None:
    """
    Main loop. Polls Github every POLL_INTERVAL_SEC seconds.
    Flow per poll cycle:
    fetch_events() -> filter -> produce() to Kafka -> sleep.
    """
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    headers = _build_headers()

    auth_status = (
        "authenticated (5000 req/h)" if GITHUB_TOKEN else "unauthenticated (60 req/h)"
    )
    logger.info(f"Producer started | Broker: {KAFKA_BROKER} | GitHub: {auth_status}")

    seen_ids: set[str] = set()  # Deduplicering inom samma session

    while True:
        events = fetch_events(headers)
        logger.info(f"Fetched {len(events)} events from Github API")

        sent = 0
        skipped_type = 0
        skipped_dupe = 0

        for event in events:
            event_id = event.get("id")

            # Hoppas över dubbletter. Github returnerar samma 300 events tills nya dyker upp.
            # Utan if satsen skickar jag samma event flera gånger.
            if event_id in seen_ids:
                skipped_dupe += 1
                continue

            if not _is_de_relevant(event):
                skipped_type += 1
                continue

            seen_ids.add(event_id)

            # produce() är icke-blockerande, den lägger msg i en intern kö
            # poll(0) ger Kafka chansen att skicka det som satts i kö.
            producer.produce(
                topic=KAFKA_TOPIC_RAW,
                key=event_id,
                value=json.dumps(event),
                callback=_on_delivery,
            )
            producer.poll(0)
            sent += 1

        # flush() blockerar tills alla köade msgs är levererade.
        # Gör det efter varje poll cykel, inte efter varje event!!
        producer.flush()

        logger.info(
            f"Cycle complete | sent={sent} skipped_type={skipped_type}, skipped_dupe={skipped_dupe}"
        )

        # Håll seen ids hanterbart, rensa om den växer sig för stor!
        # 10k IDs = ~33 poll cykler utan restart.
        if len(seen_ids) > 10_000:
            seen_ids.clear()
            logger.debug("seen_ids cache cleared!")

        logger.info(f"Sleeping {POLL_INTERVAL_SEC}s until next poll")
        time.sleep(POLL_INTERVAL_SEC)


# --- Entrypoint ---
if __name__ == "__main__":
    run_producer()
