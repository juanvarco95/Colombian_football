"""Pipeline configuration — single source of truth for the medallion pipeline.

Pipeline source files cannot `%run` notebooks, so this module mirrors the values
of `notebooks/common/02_Config` (the notebooks' source of truth). Keep both in
sync when promoting `football_col_dev` to a prod/staging target — or, later,
load them from the pipeline's Spark configuration instead.
"""

CATALOG = "football_col_dev"

# Medallion tables (fully qualified names — the pipeline publishes to each schema)
BRONZE_TABLE = f"{CATALOG}.bronze.api_fixtures"
SILVER_TABLE = f"{CATALOG}.silver.api_fixtures"
GOLD_SCHEMA = f"{CATALOG}.gold"
GOLD_FACT_TEAM_MATCH = f"{GOLD_SCHEMA}.fact_team_match"
GOLD_DIM_TEAM = f"{GOLD_SCHEMA}.dim_team"
GOLD_DIM_DATE = f"{GOLD_SCHEMA}.dim_date"

# Landing zone (UC volume): raw JSON files landed by the Landing notebook
RAW_VOLUME = f"/Volumes/{CATALOG}/landing/raw"
FIXTURES_DIR = f"{RAW_VOLUME}/api_football/fixtures"

# API-Football scope: Colombian Primera A only, seasons 2022-2024 (free plan)
LEAGUE_ID = 239