# test_transforms.py - PySpark unit tests för bronze_to_silver transformationslogik
# Kommentarer: Svenska
# Kod: Engelska

# TestTransform - testar _transform() med riktiga PySpark(Spark) DataFrames
# Kräver en lokal SparkSession (session_scoped fixture för prestanda)

# TestCheckpoint - testar _load_checkpoint() och _save_checkpoint()
# Ren Python, ingen Spark, använder mig av tmp_path och monkeypatch ifrån Pytest
import json
import pytest
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from config import BRONZE_DIR
from transforms.bronze_to_silver import (
    _load_checkpoint,
    _save_checkpoint,
    _transform,
)


# =====================================================================
# SPARK SESSION FIXTURE
#
# scope="session" = EN SparkSession delas av ALLA tester i den här filen.
# Att starta Spark kostar ~3-5 sekunder. Med session-scope betalar jag
# den kostnaden EN gång, inte en gång per test-metod.
#
# yield-mönstret (istället för return) låter pytest köra spark.stop()
# som teardown efter att ALLA tester är klara - ingen läcka av resurser.
# =====================================================================
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master(
            "local[*]"  # En tråd räcker för tester, snabbt och deterministiskt
        )
        .appName("test-bronze-to-silver")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# =====================================================================
# BRONZE SCHEMA
#
# Speglar exakt hur Bronze Parquet-filer ser ut på disk.
# actor och repo är Structs (nästlade objekt), därför kan jag använda
# punktnotation (actor.login) direkt i Spark utan get_json_object.
# payload är en JSON-sträng, därför krävs get_json_object för den.
#
# Det här schemat måste matcha consumer.py's output. Om consumer.py
# ändrar hur den skriver Bronze, måste det här schemat uppdateras.
# =====================================================================
BRONZE_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("type", StringType()),
        StructField(
            "actor",
            StructType(
                [
                    StructField("login", StringType()),
                    StructField("id", LongType()),
                ]
            ),
        ),
        StructField(
            "repo",
            StructType(
                [
                    StructField("name", StringType()),
                    StructField("id", LongType()),
                ]
            ),
        ),
        StructField("payload", StringType()),  # JSON-sträng, INTE ett dict
        StructField("created_at", StringType()),
    ]
)


# =====================================================================
# HELPER: Bygg en Bronze-rad med rimliga defaults
#
# Gör det enkelt att skapa testvarianter utan att upprepa all data.
# DRY-principen tillämpad på testdata, samma princip som pytest fixtures,
# fast som en vanlig funktion eftersom jag behöver parametrisering.
# =====================================================================
def make_bronze_row(
    event_id: str = "12345678",
    event_type: str = "PushEvent",
    actor_login: str = "johnnyhyy",
    repo_name: str = "apache/airflow",
    repo_id: int = 42,
    payload: dict | None = None,
    created_at: str = "2026-03-29T16:00:00Z",
) -> tuple:
    if payload is None:
        payload = {"size": 3}
    # Tuples matchar BRONZE_SCHEMA kolumnordning.
    # actor och repo är nästlade tuples som matchar deras StructType.
    return (
        event_id,
        event_type,
        (actor_login, 99),
        (repo_name, repo_id),
        json.dumps(payload),  # payload MÅSTE vara en JSON-sträng, inte ett dict
        created_at,
    )


# =====================================================================
# TESTER FÖR _transform()
#
# _transform() är en ren funktion: Bronze DataFrame -> Silver DataFrame
# Ingen file I/O, inga sidoeffekter. Deterministisk och lätt att testa.
# =====================================================================


class TestTransform:
    # Testar att silver schemat har exakt nr förväntade cols
    def test_silver_schema_has_exactly_expected_columns(self, spark):
        """
        The Silver schema should always have exactly these columns, no more, no less.

        This is a schema contract: if someone adds or removes
        a column in _transform() without updating this test, it will be caught
        by CI before it reaches main. The Gold models in dbt depend on
        this exact schema and will crash silently if a column is missing.
        """
        df = spark.createDataFrame([make_bronze_row()], schema=BRONZE_SCHEMA)
        result = _transform(df)

        expected_columns = {
            "event_id",
            "event_type",
            "actor_login",
            "repo_name",
            "repo_id",
            "commit_count",
            "event_action",
            "pr_merged",
            "pr_number",
            "created_at",
            "is_bot",
        }
        assert set(result.columns) == expected_columns

    # Testar att push event values är extraherade rätt
    def test_push_event_values_are_correctly_extracted(self, spark):
        """
        Checks that scalar values are extracted correctly for a PushEvent.
        actor_login and repo_name are retrieved via Struct dot notation.
        commit_count is retrieved via get_json_object(payload, '$.size').
        """
        df = spark.createDataFrame(
            [make_bronze_row(event_id="evt-abc", payload={"size": 5})],
            schema=BRONZE_SCHEMA,
        )
        row = _transform(df).collect()[0]

        assert row["event_id"] == "evt-abc"
        assert row["event_type"] == "PushEvent"
        assert row["actor_login"] == "johnnyhyy"
        assert row["repo_name"] == "apache/airflow"
        assert row["commit_count"] == 5

    # Testar dedupe logik och tar bort dupes.
    def test_deduplication_removes_duplicate_event_ids(self, spark):
        """
        Deduplication on event_id is critical for data quality.

        The bootstrap script and live producer can write the same event to
        Bronze more than once, e.g. if bootstrap covers a date range
        that the live producer has already retrieved. A duplicate in Silver breaks
        the aggregations in Gold: tool_growth counts a star twice,
        pr_cycle_times can have negative cycle times.
        """
        rows = [
            make_bronze_row(event_id="dupe-001"),
            make_bronze_row(event_id="dupe-001"),  # Exakt duplikat - ska tas bort
            make_bronze_row(event_id="unique-002"),
        ]
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        result = _transform(df)

        assert result.count() == 2

    # Test för GollumEvents(läs på mer)
    def test_unknown_event_types_are_filtered_oyt(self, spark):
        """
        GollumEvent (wiki editing) and MemberEvent should be filtered out.
        I only care about RELEVANT_EVENT_TYPES defined in config.py.
        Unknown types are noise for our Gold aggregations.
        """
        rows = [
            make_bronze_row(event_id="push-1", event_type="PushEvent"),
            make_bronze_row(event_id="gollum-1", event_type="GollumEvent"),
            make_bronze_row(event_id="member-1", event_type="MemberEvent"),
        ]
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        result = _transform(df)

        assert result.count() == 1
        assert result.collect()[0]["event_type"] == "PushEvent"

    # Test för min payload size. Är det ingenting i payload ska det by default vara 0, inte krasch eller nulls
    def test_commit_count_defaults_to_zero_when_payload_size_missing(self, spark):
        """
        If payload lacks 'size' (ex, WatchEvent, ForkEvent)
        commit_count should default to 0 via coalesce, not crash or become null.

        Null in a Gold aggregation infects the entire sum and is difficult to debug.
        0 is the semantically correct default value: no commit happened.
        """
        df = spark.createDataFrame(
            [make_bronze_row(event_type="WatchEvent", payload={})],
            schema=BRONZE_SCHEMA,
        )
        row = _transform(df).collect()[0]

        assert row["commit_count"] == 0

    # Testar pr_nr för mina gold marts. Utan pr_number == issue med crossjoin som jag stött på tidigare
    def test_pr_number_extracted_from_nested_payload(self, spark):
        """
        pr_number is critical for the pr_cycle_times Gold models.

        Without pr_number, the self-join in pr_cycle_times creates a Cartesian
        product (M×N) when matching opened-events against closed-events on
        only repo_name. With pr_number, the join is 1:1 per PR.

        Retrieved via get_json_object(payload, '$.pull_request.number').
        """
        payload = {
            "action": "opened",
            "pull_request": {"number": 42, "merged": False},
        }
        df = spark.createDataFrame(
            [make_bronze_row(event_type="PullRequestEvent", payload=payload)],
            schema=BRONZE_SCHEMA,
        )
        row = _transform(df).collect()[0]

        assert row["pr_number"] == 42
        assert row["event_action"] == "opened"

    # Test för pr merges som varit problematiskt innan
    def test_pr_merged_is_always_false_due_to_github_api_quirk(self, spark):
        """
        Github's API does NOT put pull_request.merged in the payload for
        PullRequestEvent with action='merged'. get_json_object returns
        null for that key, and coalesce falls back to False for
        ALL rows, even those that are actually merged.

        The pr_merged column is practically useless for identifying
        merged PRs. Use event_action = 'merged' instead.

        This test exists to DOCUMENT the behavior in code,
        not because it is desirable. If GitHub ever changes its API
        and starts setting the merged flag correctly, this test
        should be updated, and the pr_merged column can be used.
        """
        payload = {
            "action": "merged",
            "pull_request": {"number": 7},
        }
        df = spark.createDataFrame(
            [make_bronze_row(event_type="PullRequestEvent", payload=payload)],
            schema=BRONZE_SCHEMA,
        )
        row = _transform(df).collect()[0]

        # pr_merged är False trots att payload säger True - GitHub API-quirk
        assert row["pr_merged"] is False
        # event_action är däremot korrekt, det rätta sättet att hitta merged PRs
        assert row["event_action"] == "merged"

    # test för partitionkeys med zero-padding
    # se hive-style partitioning. Rätt format = month=02, day=09
    # inte month=3, day=9
    def test_partition_keys_are_zero_padded(self, spark):
        """
        Partition keys MUST be zero-padded: month=03, day=07.

        Without zero-padded, PySpark creates inconsistent partitions in the filesystem:
        'month=3' and 'month=03' are two separate folders but represent
        the same logical partition. PySpark cannot merge them on read,
        which results in incorrect aggregations in Gold.

        This test catches a regression I have already bugged and fixed,
        that kind of bug is worth protecting with an explicit test.
        """
        df = spark.createDataFrame(
            [make_bronze_row(created_at="2026-03-07T10:00:00Z")],
            schema=BRONZE_SCHEMA,
        )
        result = (
            _transform(df)
            .withColumn("month", F.date_format(F.to_timestamp("created_at"), "MM"))
            .withColumn("day", F.date_format(F.to_timestamp("created_at"), "dd"))
        )
        row = result.collect()[0]

        assert row["month"] == "03"  # Inte "3"
        assert row["day"] == "07"  # Inte "7"


# =====================================================================
# TESTER FÖR CHECKPOINT FUNKTIONER
#
# _load_checkpoint() och _save_checkpoint() är ren Python utan Spark.
# Testar file I/O med pytest's tmp_path (temporär mapp per test) och
# monkeypatch (byt ut konstanter tillfälligt utan att ändra källkod)
#
# tmp_path:
# pytest skapar en unik temporär mapp per test och städar
# upp den automatiskt efter körning. Inga sidoeffekter.
#
# monkeypatch:
# byter tillfälligt ut BRONZE_SILVER_CHECKPOINT-konstanten
# i modulen så att testerna skriver till tmp_path istället
# för det riktiga checkpoint pathen.
# =====================================================================
class TestCheckpoint:
    # Test för checkpoint file logik.
    def test_load_returns_empty_set_when_no_file_exists(self, tmp_path, monkeypatch):
        """
        First run: no checkpoint file found on disk.
        Should return an empty set and log 'No checkpoint found'.
        Should NOT crash with FileNotFoundError.
        """
        monkeypatch.setattr(
            "transforms.bronze_to_silver.BRONZE_SILVER_CHECKPOINT",
            tmp_path / "nonexistent.json",
        )
        result = _load_checkpoint()

        assert result == set()

    # Test för att att sparade filer faktiskt är exakt mängd av sparade filer.
    # Verifierar json serialisering och deserialisering
    def test_save_and_load_roundtrip_preserves_file_count(self, tmp_path, monkeypatch):
        """
        Save X files -> reload -> should still be X files.
        Verifies that JSON serialization and deserialization are correct
        and that no information is lost in the round trip to disk.
        """
        checkpoint_path = tmp_path / "checkpoint.json"
        monkeypatch.setattr(
            "transforms.bronze_to_silver.BRONZE_SILVER_CHECKPOINT",
            checkpoint_path,
        )

        fake_files = {
            str(BRONZE_DIR / "year=2026/month=01/day=01/a.parquet"),
            str(BRONZE_DIR / "year=2026/month=01/day=02/b.parquet"),
            str(BRONZE_DIR / "year=2026/month=01/day=03/c.parquet"),
        }
        _save_checkpoint(fake_files)
        loaded = _load_checkpoint()

        assert len(loaded) == 3

    # Test för att säkerställa att checkpointfilen lagrar RELATIVA paths
    # ALDRIG ABSOLUTE PATHS
    def test_checkpoint_stores_relative_not_absolute_paths(self, tmp_path, monkeypatch):
        """
        Absolute paths are machine-specific and environment-specific:
        - Local: C:/Users/***/Desktop/folder/.../a.parquet
        - Docker: /app/data/bronze/events/.../a.parquet

        Relative paths (year=2026/month=01/day=01/a.parquet) are identical
        in all environments and resolve correctly to BRONZE_DIR on load.

        This test is made to protects against a bug thats already debugged and fixed:
        the checkpoint file written locally never matched files found
        inside the Docker container, which gave a cold run on every DAG run in Airflow.
        """
        checkpoint_path = tmp_path / "checkpoint.json"
        monkeypatch.setattr(
            "transforms.bronze_to_silver.BRONZE_SILVER_CHECKPOINT",
            checkpoint_path,
        )

        fake_files = {str(BRONZE_DIR / "year=2026/month=01/day=01/a.parquet")}
        _save_checkpoint(fake_files)

        with open(checkpoint_path) as f:
            data = json.load(f)

        for saved_path in data["processed_files"]:
            assert not Path(saved_path).is_absolute(), (
                f"Absolute path found in checkpoint: {saved_path}\n"
                f"Checkpoint should store Relative paths to work "
                f"Correct in both local environment and docker."
            )

    # Test för att se så att checkpointfilen ALLTID returnerar / slashes och aldrig backslashes
    def test_saved_paths_always_use_forward_slashes(self, tmp_path, monkeypatch):
        """
        The checkpoint file should ALWAYS store forward slashes, never backslashes.
        str(Path(...)) returns backslashes on Windows, as_posix() always returns
        forward slashes regardless of OS. If you mix them in the same file,
        Docker runs will miss all Windows-written entries and cold run unnecessarily.
        """
        checkpoint_path = tmp_path / "checkpoint.json"
        monkeypatch.setattr(
            "transforms.bronze_to_silver.BRONZE_SILVER_CHECKPOINT",
            checkpoint_path,
        )

        fake_files = {str(BRONZE_DIR / "year=2026/month=06/day=13/test.parquet")}
        _save_checkpoint(fake_files)

        with open(checkpoint_path) as f:
            data = json.load(f)

        for saved_path in data["processed_files"]:
            assert "\\" not in saved_path, (
                f"Backslash hittad i checkpoint: {saved_path}\n"
                f"Använd as_posix() istället för str(Path(...)) vid sparning."
            )
