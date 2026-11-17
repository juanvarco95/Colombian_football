# Databricks notebook source
# DBTITLE 1,Notebook documentation
# MAGIC %md
# MAGIC # Landing (API ingestion)
# MAGIC
# MAGIC Downloads Colombian football fixtures from the API-Football API and lands the raw JSON responses as files in Unity Catalog volumes.
# MAGIC
# MAGIC * **Source:** `https://v3.football.api-sports.io` — league 239 (Colombian Primera A), all available seasons
# MAGIC * **Target:** `/Volumes/football_col_dev/landing/raw/api_football/fixtures/` (one timestamped JSON file per season)
# MAGIC
# MAGIC **How it works**
# MAGIC 1. `%pip install requests` and restart the Python environment.
# MAGIC 2. Loads shared imports and the run logger from `../common/setup`.
# MAGIC 3. Discovers all available seasons for league 239 and calls the `fixtures` endpoint once per season, writing one timestamped JSON file per season.
# MAGIC 4. Every API call and every landed file is logged.
# MAGIC
# MAGIC **Run:** top to bottom. Requires the `football_scope/api_football_key` secret.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load shared setup (imports + logger)
# MAGIC %run ../common/Setup

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

# DBTITLE 1,Init run logger
logger = get_logger("landing")

# COMMAND ----------

# DBTITLE 1,API and volume configuration
BASE_URL = 'https://v3.football.api-sports.io'
HEADERS = {"x-apisports-key": dbutils.secrets.get("football_scope", "api_football_key")}
LANDING = "/Volumes/football_col_dev/landing/raw/api_football"

logger.info(f"Landing run started | API: {BASE_URL} | target volume: {LANDING}")

# COMMAND ----------

# DBTITLE 1,fetch_and_land: call API and write JSON to landing
def fetch_and_land(
    endpoint: str,
    params: dict
) -> str:
    response = requests.get(
        url     = f"{BASE_URL}/{endpoint}",
        headers = HEADERS,
        params  = params,
        timeout = 30
    )
    response.raise_for_status()

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    suffix = '_'.join(f'{k}-{v}' for k, v in params.items())
    path = f'{LANDING}/{endpoint}/{suffix}_{ts}.json'

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(response.json(), f)

    logger.info(f"Landed {endpoint} ({suffix}) -> {path}")

    return path


# COMMAND ----------

# DBTITLE 1,get_league_seasons: list available seasons
def get_league_seasons(
    league_id: int
) -> list[int]:
    """Return all available seasons for a given league from the API-Football API."""
    response = requests.get(
        url     = f"{BASE_URL}/leagues",
        headers = HEADERS,
        params  = {"id": league_id},
        timeout = 30
    )
    response.raise_for_status()

    seasons = [
        season["year"]
        for season in response.json()["response"][0]["seasons"]
    ]
    logger.info(f"League {league_id}: {len(seasons)} seasons available")
    return seasons


# COMMAND ----------

# DBTITLE 1,Fetch and land all league seasons
seasons = get_league_seasons(239)
logger.info(f"Fetching fixtures for {len(seasons)} seasons of league 239")

for season in seasons:
    logger.info(f"Fetching fixtures for season {season}")
    fetch_and_land(
        "fixtures",
        {
            "league": 239,
            "season": season
        }
    )

logger.info("Landing run completed: all seasons fetched and landed")