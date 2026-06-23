import duckdb
import os
from pathlib import Path


# Projektrot beräknad relativt skriptets EGEN plats -- oberoende av varifrån
# du faktiskt kör `uv run python ...` ifrån. Mer robust än att lita på cwd.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "dbt" / "github_lake.duckdb"
DBT_DIR = PROJECT_ROOT / "dbt"  # motsvarar /app/dbt i containern

con = duckdb.connect(str(DB_PATH), read_only=True)

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
