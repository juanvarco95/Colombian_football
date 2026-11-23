"""Pipeline configuration — single source of truth for notebooks AND the pipeline.

The catalog is resolved in this order:
1. FOOTBALL_CATALOG environment variable (local tests / manual override),
2. the `football.catalog` pipeline configuration key (set by the bundle's
   pipeline resource from its ${catalog} variable — dev and prod targets),
3. default `football_col_dev` (notebooks running in the dev workspace).

Notebooks never hardcode names: `notebooks/common/02_Config` is a thin shim
that imports this module, so both surfaces always agree. `for_catalog()`
re-derives every constant for any catalog (the shim uses it to honor the
`catalog` job/notebook parameter).
"""

import os
from types import SimpleNamespace

DEFAULT_CATALOG = "football_col_dev"

# API-Football scope (free plan: 10 requests/minute, 100/day, seasons 2022-2024)
API_BASE_URL = "https://v3.football.api-sports.io"
LEAGUE_ID = 239  # Colombian Primera A
SECRET_SCOPE = "football_scope"
SECRET_KEY = "api_football_key"
SECONDS_BETWEEN_CALLS = 7
RATE_LIMIT_WAIT = 60

# Workspace layout: standalone SQL scripts (runnable directly in the SQL editor)
PROJECT_ROOT = "/Workspace/Users/juanvarco95@gmail.com/Colombian_football"
SQL_DIR = f"{PROJECT_ROOT}/sql"


def resolve_catalog() -> str:
    """Resolve the target catalog (env var > pipeline config > dev default)."""
    env = os.environ.get("FOOTBALL_CATALOG")
    if env:
        return env
    try:
        # Set by the bundle's pipeline resource: configuration: football.catalog
        return spark.conf.get("football.catalog")  # noqa: F821 (spark is injected in notebooks/pipelines)
    except Exception:
        return DEFAULT_CATALOG


def for_catalog(catalog: str) -> SimpleNamespace:
    """Every pipeline constant (tables, volumes, paths) for one catalog."""
    raw_volume = f"/Volumes/{catalog}/landing/raw"
    return SimpleNamespace(
        CATALOG=catalog,
        # Medallion tables (fully qualified names — the pipeline publishes to each schema)
        BRONZE_TABLE=f"{catalog}.bronze.api_fixtures",
        SILVER_TABLE=f"{catalog}.silver.api_fixtures",
        GOLD_SCHEMA=f"{catalog}.gold",
        GOLD_FACT_TEAM_MATCH=f"{catalog}.gold.fact_team_match",
        GOLD_DIM_TEAM=f"{catalog}.gold.dim_team",
        GOLD_DIM_DATE=f"{catalog}.gold.dim_date",
        # Landing zone (UC volume): raw files, Auto Loader schema and checkpoints
        RAW_VOLUME=raw_volume,
        API_FOOTBALL_DIR=f"{raw_volume}/api_football",
        FIXTURES_DIR=f"{raw_volume}/api_football/fixtures",
        SCHEMA_LOCATION=f"{raw_volume}/_schemas/fixtures",
        CHECKPOINT_LOCATION=f"{raw_volume}/_checkpoints/bronze_fixtures",
    )


# Module-level constants (what `from common.config import X` exposes) —
# resolved once at import time for the pipeline's catalog.
_cfg = for_catalog(resolve_catalog())
CATALOG = _cfg.CATALOG
BRONZE_TABLE = _cfg.BRONZE_TABLE
SILVER_TABLE = _cfg.SILVER_TABLE
GOLD_SCHEMA = _cfg.GOLD_SCHEMA
GOLD_FACT_TEAM_MATCH = _cfg.GOLD_FACT_TEAM_MATCH
GOLD_DIM_TEAM = _cfg.GOLD_DIM_TEAM
GOLD_DIM_DATE = _cfg.GOLD_DIM_DATE
RAW_VOLUME = _cfg.RAW_VOLUME
API_FOOTBALL_DIR = _cfg.API_FOOTBALL_DIR
FIXTURES_DIR = _cfg.FIXTURES_DIR
SCHEMA_LOCATION = _cfg.SCHEMA_LOCATION
CHECKPOINT_LOCATION = _cfg.CHECKPOINT_LOCATION