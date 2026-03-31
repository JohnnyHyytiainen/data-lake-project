# Bootstrap_historical script för att få ner historisk data ifrån github archives
# Kommentarer: Svenska
# Kod: Engelska

import argparse
import gzip
import io
import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from config import (
    BRONZE_DIR,
    DATE_PARTITION_FORMAT,
    DE_KEYWORDS,
    LOG_LEVEL,
    RELEVANT_EVENT_TYPES,
)


# ========== Logging ==========
logger.remove()
logger.add(
    sink=lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | {message}",
)

# ===== Github archive URL format - En fil per timme
# https://data.gharchive.org/YYYY-MM-DD-H.json.gz
# Note: Timmen är utan noll-padding (0-23 och INTE 00-23)
GH_ARCHIVE_URL = "https://data.gharchive.org/{date}-{hour}.json.gz"


# ========== URL generering ==========
# Privat funktion, fuck around and find out..
def _generate_urls(start: datetime, end: datetime) -> list[tuple[str, datetime]]:
    """
    Generates a list of (url, timestamp) tuples for each hour
    between start and end. One hour = one file on GitHub Archive.

    We return the timestamp along with the URL so that we can
    set the correct partition path for Bronze without parsing the URL again.
    """
    urls = []
    current = start.replace(minute=0, second=0, microsecond=0)

    while current < end:
        url = GH_ARCHIVE_URL.format(
            date=current.strftime("%Y-%m-%d"),
            hour=current.hour,  # Ingen noll-padding, GH archive convention
        )
        urls.append((url, current))
        current += timedelta(hours=1)

    return urls


# ========== Filtrering ==========
# Samma filtreringslogik som producer.py.
# Ska följa samma konsistens genom hela pipen.
# Priv funktion FAAFO
def _is_relevant(event: dict) -> bool:
    """
    Same filtering logic as producer.py. It is important to keep consistency
    throughout the pipeline. Bronze from bootstrap should look identical
    to the Silver transformation as Bronze from Kafka consumer.
    """
    event_type = event.get("type", "")
    repo_name = event.get("repo", {}).get("name", "").lower()

    if event_type not in RELEVANT_EVENT_TYPES:
        return False

    return any(keyword in repo_name for keyword in DE_KEYWORDS)


# ========== Nedladdning och parsing ==========
# Priv funktion FAAFO
def _fetch_and_filter(url: str) -> list[dict]:
    """
    Downloads a .json.gz file from the GitHub Archive directly into memory,
    unpacks it, and filters out DE-relevant events.

    I never write the raw .gz file to disk, everything happens in memory.
    This saves disk space and makes the script faster because I don't
    have to do an extra I/O operation for each hour file.

    The GitHub Archive format is NDJSON (Newline Delimited JSON(JSONL)),
    one JSON line per event, not a large JSON array. This allows me
    to parse line by line without loading the entire file into memory at once.
    """
    try:
        response = requests.get(url, timeout=30)

        # 404 betyder att fil inte finns än. T.ex om jag frågar om framtida timmar
        # Eller om Github Archive har ett gap i sin historik
        if response.status_code == 404:
            logger.warning(f"File not found 404: {url}")
            return []

        response.raise_for_status()

        # gzip.decompress() packar upp bytes in memory
        # io.BytesIO() låter mig läsa de upppackade bytes som en fil!
        decompressed = gzip.decompress(response.content)
        relevant_events = []

        for line in io.BytesIO(decompressed):
            line = line.strip()
            if not line:
                continue
            try:
                event = json.loads(line)
                if _is_relevant(event):
                    relevant_events.append(event)
            except json.JSONDecodeError:
                # Korrupta rader kan förekomma men ignoreras tyst
                continue

        return relevant_events

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return []


# ========== Parquet writing ==========
# Skriver lista av events till bronze med hive style partitioning
# Privat funktion FAAFO
def _write_to_bronze(events: list[dict], timestamp: datetime) -> None:
    """
    Writes a list of events to Bronze with Hive-style partitioning.
    The timestamp parameter controls which folder the events end up in, it
    represents the hour the file came from in the GitHub Archive.
    """
    if not events:
        return

    partition = DATE_PARTITION_FORMAT.format(
        year=timestamp.year,
        month=timestamp.month,
        day=timestamp.day,
    )
    # Vart min output path är där filer ska in.
    # Skapa folder om den ej finns.
    output_path = BRONZE_DIR / partition
    output_path.mkdir(parents=True, exists_ok=True)

    table = pa.Table.from_pylist(events)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    output_file = output_path / f"bootstrap-{ts}.parquet"

    pq.write_table(table, output_file, compression="snappy")
    logger.info(f"Wrote {len(events)} events -> {output_file}")


# ========== Huvudfunktionen ==========
def run_bootstrap(start: datetime, end: datetime) -> None:
    """ """
