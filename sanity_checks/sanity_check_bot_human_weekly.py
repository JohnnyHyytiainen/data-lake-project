# Sanity check script innan implementering av grafana panelen
#
# Syfte: Möta volymen per vecka innan minsta volume threshold för panel väljs +
# bevisa omräkningsteknik där jag undviker att använda AVG och få ut fel % i resultatet.
#
# bot_vs_human_activity är en TABLE och inte en VIEW, ingen os.chdir() behövs här.
import duckdb
import pandas as pd
from pathlib import Path


pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_rows", None)

# Root sätts relativt till vart skriptet är placerat, spelar ingen roll vart det körs ifrån
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "data" / "dbt" / "github_lake.duckdb"
# Skapa anslutning till db
con = duckdb.connect(str(DB_PATH), read_only=True)

# Castar till BIGINT för att hålla exakt samma SQL form som Grafana kommer kräva.
# DuckDB SUM till HUGEINT kan inte Grafanas drivers läsa
#
# ===== 1: Täckning per vecka =====
# Hur många veckor finns det data för och över vilket spann?
print("===== 1: Weekly coverage =====")
print(
    con.execute("""
    SELECT
        COUNT(DISTINCT week_start) AS num_weeks,
        MIN(week_start) AS first_week,
        MAX(week_start) AS last_week,
        CAST(SUM(event_count) AS BIGINT) AS total_events
    FROM bot_vs_human_activity
""").fetchdf()
)

# ===== 2: Antal events per vecka i kronologisk ordning =====
# Försök till att hitta veckor med mindre aktivitet, målet är
# att försöka stänga luckor där veckor med få antal events inte
# visuellt ser likadana ut som veckor med massvis av events
print("\n===== 2: Events per week (chronologically) =====")
print(
    con.execute("""
    SELECT
        week_start,
        CAST(SUM(event_count) AS BIGINT) AS events_in_week
    FROM bot_vs_human_activity
    GROUP BY week_start
    ORDER BY week_start
""").fetchdf()
)

# ===== 3: Fördelning av volum per vecka =====
# Menat att ge underlag att sätta ett tröskelvärde på en RIKTIG SIFFRA.
print("\n===== 3: Distribution over weekly volume =====")
print(
    con.execute("""
    WITH weekly AS (
        SELECT week_start, SUM(event_count) AS events_in_week
        FROM bot_vs_human_activity
        GROUP BY week_start
    )
    SELECT
        CAST(MIN(events_in_week) AS BIGINT) AS min_events,
        CAST(quantile_cont(events_in_week, 0.10) AS BIGINT) AS p10,
        CAST(quantile_cont(events_in_week, 0.20) AS BIGINT) AS p20,
        CAST(quantile_cont(events_in_week, 0.25) AS BIGINT) AS p25,
        CAST(quantile_cont(events_in_week, 0.50) AS BIGINT) AS p50,
        CAST(quantile_cont(events_in_week, 0.90) AS BIGINT) AS p90,
        CAST(MAX(events_in_week) AS BIGINT) AS max_events
    FROM weekly
""").fetchdf()
)


# ===== 4: Panelens query =====
# Grain från week, event_type, actor_category TILL
# week, actor_category.
# event_count är DISTRIBUTIVT och får summeras vidare
# pct_of_week_type SKA INTE RÖRAS, de räknas om FRÅN GRUNDEN HÄR
print("\n===== 4: Query for Grafan panel, latest 24 rows =====")
print(
    con.execute("""
    SELECT
        week_start,
        actor_category,
        CAST(SUM(event_count) AS BIGINT) AS events,
        ROUND(
            100.0 * SUM(event_count)
                  / SUM(SUM(event_count)) OVER (PARTITION BY week_start),
            2
        ) AS pct_of_week
    FROM bot_vs_human_activity
    GROUP BY week_start, actor_category
    ORDER BY week_start DESC, pct_of_week DESC
    LIMIT 24

""").fetchdf()
)

# ===== 5: InvarianT, summeras andelarna till 100% per vecka eller ej? =====
# Förväntade svaret är: 0, alla andra resultat innebär att beräkningen är fel någonstans.
print("\n===== 5: Invariant. Weeks that does NOT sum to 100% in total =====")
print(
    con.execute("""
    WITH regrained AS (
        SELECT
            week_start,
            actor_category,
            100.0 * SUM(event_count)
                  / SUM(SUM(event_count)) OVER (PARTITION BY week_start) AS pct
        FROM bot_vs_human_activity
        GROUP BY week_start, actor_category
    ),
    per_week AS (
        SELECT week_start, SUM(pct) AS total_pct
        FROM regrained
        GROUP BY week_start
    )
    SELECT COUNT(*) AS weeks_not_summing_to_100
    FROM per_week
    WHERE ABS(total_pct - 100.0) > 0.01
""").fetchdf()
)


# ===== 6: Sökande efter bevis:  AVG(pct) vs korrekt omräkning =====
# Samma vecka men två olika metoder.
# Skillnaden mellan kolumnerna här är viktnings felet,
# Menat att mätas på RIKTIG data och inte fake data.
print("\n===== 6: AVG(pct) vs correctly recalculated data the last week =====")
print(
    con.execute("""
    WITH latest AS (
        SELECT MAX(week_start) AS w FROM bot_vs_human_activity
    )
    SELECT
        b.actor_category,
        ROUND(AVG(b.pct_of_week_type), 2) AS wrong_avg_of_pct,
        ROUND(
            100.0 * SUM(b.event_count) / SUM(SUM(b.event_count)) OVER (),
            2
        ) AS correct_pct
    FROM bot_vs_human_activity b, latest
    WHERE b.week_start = latest.w
    GROUP BY b.actor_category
    ORDER BY correct_pct DESC
""").fetchdf()
)

# Stäng anslutningen fint.
con.close()
