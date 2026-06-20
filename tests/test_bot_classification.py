# Kommentarer: Svenska
# Kod: Engelska
# Script för att testa min bot classification logik

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from transforms.bronze_to_silver import _classify_is_bot


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("test-is-bot").getOrCreate()


def test_is_bot_classification(spark):
    test_cases = [
        # === TRUE: officiella suffix/prefix ===
        ("github-actions[bot]", True),
        ("dependabot[bot]", True),
        ("renovate[bot]", True),
        ("Lovable[bot]", True),
        ("lovable-dev[bot]", True),  # Lovables riktiga bot user
        ("aws-airflow-bot", True),
        ("renovate-bot", True),
        ("My-Bot", True),
        ("my_bot", True),
        ("DePeNdAbot", True),  # Case insensitive + dependabot-prefix
        ("RENOVATE-ci-runner", True),  # Renovate-prefix isolerat från [bot] / -bot
        ("github-actions-deploy", True),  # github-actions-prefix, isolerat
        # === FALSE: Medvetna gränsfall ===
        ("abbot", False),  # "bot" UTAN separator
        ("flinkbot", False),  # Käns accepterad begränsning
        ("JohnBOTjovi", False),  # "bot" i mitten av string, ej suffix
        ("myrenovatebot", False),  # "renovate" mitt i sring, ej prefix
        ("Johnny", False),  # Ingen träff, enbart random namn
        # === FALSE: För att säkerställa att det funkar som tänkt ===
        (None, False),  # Null ska _ALDRIG_ krascha, defaultar False
    ]

    df = spark.createDataFrame(test_cases, ["actor_login", "expected_is_bot"])
    result = df.withColumn("is_bot", _classify_is_bot(F.col("actor_login")))

    for row in result.collect():
        assert row.is_bot == row.expected_is_bot, (
            f"Fel för actor_login={row.actor_login!r}"
        )
