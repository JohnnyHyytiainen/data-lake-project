import duckdb
import os
from pathlib import Path

# Hitta "tröskelvärdet" för vad som är ett mänskligt beteende vs bot-beteende på GitHub.

# Projektrot beräknad relativt skriptets EGEN plats -- oberoende av varifrån
# du faktiskt kör `uv run python ...` ifrån. Mer robust än att lita på cwd.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "dbt" / "github_lake.duckdb"
DBT_DIR = PROJECT_ROOT / "dbt"  # motsvarar /app/dbt i containern

con = duckdb.connect(str(DB_PATH), read_only=True)

# int_actor_behavior är en VIEW byggd ovanpå stg_github_events, som
# innehåller en relativ path. Den pathen löses mot FRÅGARENS arbetskatalog
# vid frågetillfället -- inte mot var viewn en gång skapades. dbt-containern
# löser detta gratis via working_dir=/app/dbt. Kör lokalt, utanför
# containern -- så jag simulerar samma arbetskatalog manuellt, en gång,
# innan jag rör viewn.
os.chdir(DBT_DIR)

bot_human_result = con.sql("""
SELECT
    CASE
        WHEN is_bot THEN 'bot'
        WHEN (unique_repos = 1 AND unique_event_types = 1 AND event_count > 500)
            THEN 'suspected_automation'
        ELSE 'human'
    END AS category,
    count(*) AS num_actors
FROM int_actor_behavior
GROUP BY category
ORDER BY num_actors DESC
""")

print(bot_human_result)
con.close()
