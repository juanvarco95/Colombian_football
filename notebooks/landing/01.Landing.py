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
# MAGIC 1. Uses the preinstalled `requests` library (no `%pip install` needed on serverless).
# MAGIC 2. Loads shared setup (imports + logger) via `%run ../common/01.Setup`.
# MAGIC 3. Discovers all available seasons for league 239 and calls the `fixtures` endpoint once per season, writing one timestamped JSON file per season.
# MAGIC 4. Every API call and every landed file is logged.
# MAGIC
# MAGIC **Run:** top to bottom. Requires the `football_scope/api_football_key` secret.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load shared setup (imports + logger)
# MAGIC %run ../common/01.Setup

# COMMAND ----------

# DBTITLE 1,Init run logger
logger = get_logger("landing")

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("seasons", "", "Seasons (comma-separated, empty = auto)")

# COMMAND ----------

# DBTITLE 1,API and volume configuration
BASE_URL  = "https://v3.football.api-sports.io"
HEADERS   = {"x-apisports-key": dbutils.secrets.get("football_scope", "api_football_key")}
LANDING   = "/Volumes/football_col_dev/landing/raw/api_football"
LEAGUE_ID = 239

SECONDS_BETWEEN_CALLS = 7      # free plan allows 10 requests per minute
RATE_LIMIT_WAIT       = 60     # seconds to wait if the per-minute limit is hit anyway

logger.info(f"Landing run started | API: {BASE_URL} | target volume: {LANDING}")

# COMMAND ----------

# DBTITLE 1,call_api: one request with API-level error handling
class PlanRestrictedError(Exception):
    """The API plan does not cover the requested data (e.g. a season outside the free range)."""


def call_api(
    endpoint: str,
    params: dict,
    retry_on_rate_limit: bool = True
) -> dict:
    response = requests.get(
        url     = f"{BASE_URL}/{endpoint}",
        headers = HEADERS,
        params  = params,
        timeout = 30
    )
    # Transport-level errors (401, 500, ...)
    response.raise_for_status()

    payload = response.json()

    # API-level errors arrive with HTTP 200: "errors" is [] when OK,
    # or a dict such as {"plan": "..."}, {"rateLimit": "..."}, {"requests": "..."}
    errors = payload.get("errors")
    if errors:
        if isinstance(errors, list):
            errors = {"error": "; ".join(map(str, errors))}

        if "plan" in errors:
            raise PlanRestrictedError(errors["plan"])

        if "rateLimit" in errors and retry_on_rate_limit:
            logger.warning(f"Per-minute rate limit hit, waiting {RATE_LIMIT_WAIT}s: {errors['rateLimit']}")
            time.sleep(RATE_LIMIT_WAIT)
            return call_api(endpoint, params, retry_on_rate_limit=False)

        # Daily quota exhausted, invalid key, bad parameters...: stop the run
        raise RuntimeError(f"API error on {endpoint} {params}: {errors}")

    remaining = response.headers.get("x-ratelimit-requests-remaining")
    if remaining is not None:
        logger.info(f"Daily requests remaining: {remaining}")

    return payload

# COMMAND ----------

# DBTITLE 1,fetch_and_land: call API and write JSON to landing
def fetch_and_land(
    endpoint: str,
    params: dict
) -> str | None:
    payload = call_api(endpoint, params)

    # Never land empty responses: they waste space and hide problems
    if payload.get("results", 0) == 0:
        logger.warning(f"No data returned for {endpoint} ({params}); nothing landed")
        return None

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    suffix = '_'.join(f'{k}-{v}' for k, v in params.items())
    path = f'{LANDING}/{endpoint}/{suffix}_{ts}.json'

    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(payload, f)

    logger.info(f"Landed {endpoint} ({suffix}) -> {path} | {payload['results']} records")

    return path

# COMMAND ----------

# DBTITLE 1,get_league_seasons: list available seasons
def get_league_seasons(
    league_id: int
) -> list[dict]:
    """Return every season of the league with its 'current' flag."""
    payload = call_api("leagues", {"id": league_id})

    seasons = [
        {"year": season["year"], "current": season["current"]}
        for season in payload["response"][0]["seasons"]
    ]
    logger.info(f"League {league_id}: {len(seasons)} seasons available")
    return seasons

# COMMAND ----------

# DBTITLE 1,get_landed_seasons: seasons already landed with data
SEASON_IN_FILENAME = re.compile(r"season-(\d{4})_")


def get_landed_seasons(
    endpoint: str
) -> set[int]:
    """Seasons that already have at least one landed file with real data."""
    folder = f"{LANDING}/{endpoint}"
    if not os.path.exists(folder):
        return set()

    landed = set()
    for name in os.listdir(folder):
        match = SEASON_IN_FILENAME.search(name)
        if not match:
            continue
        with open(f"{folder}/{name}") as f:
            if json.load(f).get("results", 0) > 0:
                landed.add(int(match.group(1)))

    return landed

# COMMAND ----------

# DBTITLE 1,Decide which seasons to fetch
seasons_param = dbutils.widgets.get("seasons").strip()

if seasons_param:
    # Manual override, e.g. "2022,2023"
    seasons_to_fetch = sorted(int(s) for s in seasons_param.split(","))
    logger.info(f"Seasons forced by parameter: {seasons_to_fetch}")
else:
    seasons = get_league_seasons(LEAGUE_ID)
    time.sleep(SECONDS_BETWEEN_CALLS)

    landed = get_landed_seasons("fixtures")

    # Finished seasons don't change: fetch only the missing ones, plus the current one
    seasons_to_fetch = sorted(
        s["year"] for s in seasons
        if s["current"] or s["year"] not in landed
    )
    logger.info(
        f"{len(landed)} seasons already landed | "
        f"{len(seasons_to_fetch)} to fetch: {seasons_to_fetch}"
    )

# COMMAND ----------

# DBTITLE 1,Fetch and land
summary = {"landed": [], "empty": [], "plan_restricted": []}

for season in seasons_to_fetch:
    try:
        path = fetch_and_land(
            "fixtures",
            {
                "league": LEAGUE_ID,
                "season": season
            }
        )
        summary["landed" if path else "empty"].append(season)
    except PlanRestrictedError as e:
        logger.warning(f"Season {season} skipped, not covered by the API plan: {e}")
        summary["plan_restricted"].append(season)

    time.sleep(SECONDS_BETWEEN_CALLS)

logger.info(
    f"Landing run completed | landed: {summary['landed']} | "
    f"empty: {summary['empty']} | plan restricted: {summary['plan_restricted']}"
)