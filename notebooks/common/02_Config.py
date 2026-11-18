# Databricks notebook source
# DBTITLE 1,Notebook documentation
# MAGIC %md
# MAGIC
# MAGIC # Pipeline configuration
# MAGIC
# MAGIC The single source of truth for every name in the project: catalog, schemas,
# MAGIC tables, volume paths, API settings and workspace paths.
# MAGIC
# MAGIC * No catalog, schema, table, path or API value may be hardcoded in any notebook
# MAGIC   other than this one (`01_Setup` holds imports + logger).
# MAGIC * Every pipeline notebook loads these constants via `%run ../common/02_Config`.
# MAGIC * Change a value here once (e.g. promoting `football_col_dev` → `football_col_prod`)
# MAGIC   and every notebook picks it up on its next run.

# COMMAND ----------

# DBTITLE 1,Load shared setup (imports + logger)
# MAGIC %run ../common/01_Setup

# COMMAND ----------

# DBTITLE 1,Configuration constants (single source of truth)
# ---------------------------------------------------------------------------
# Pipeline configuration — single source of truth.
#
# The only place where catalog, schema, table, volume, API or workspace names
# may be hardcoded. Every other notebook loads these constants via
# `%run ../common/02_Config` and never hardcodes a name itself.
# ---------------------------------------------------------------------------

CATALOG = "football_col_dev"

# Medallion tables (fully qualified names)
BRONZE_TABLE = f"{CATALOG}.bronze.api_fixtures"
SILVER_TABLE = f"{CATALOG}.silver.api_fixtures"
GOLD_SCHEMA = f"{CATALOG}.gold"
GOLD_FACT_TEAM_MATCH = f"{GOLD_SCHEMA}.fact_team_match"
GOLD_DIM_TEAM = f"{GOLD_SCHEMA}.dim_team"
GOLD_DIM_DATE = f"{GOLD_SCHEMA}.dim_date"

# Landing zone (UC volume): raw files, Auto Loader schema and checkpoints
RAW_VOLUME = f"/Volumes/{CATALOG}/landing/raw"
API_FOOTBALL_DIR = f"{RAW_VOLUME}/api_football"
FIXTURES_DIR = f"{API_FOOTBALL_DIR}/fixtures"
SCHEMA_LOCATION = f"{RAW_VOLUME}/_schemas/fixtures"
CHECKPOINT_LOCATION = f"{RAW_VOLUME}/_checkpoints/bronze_fixtures"

# API-Football (free plan: 10 requests/minute, 100/day, seasons 2022-2024)
API_BASE_URL = "https://v3.football.api-sports.io"
LEAGUE_ID = 239  # Colombian Primera A
SECRET_SCOPE = "football_scope"
SECRET_KEY = "api_football_key"
SECONDS_BETWEEN_CALLS = 7
RATE_LIMIT_WAIT = 60

# Workspace layout: standalone SQL scripts executed by the gold notebook
PROJECT_ROOT = "/Workspace/Users/juanvarco95@gmail.com/Colombian_football"
SQL_DIR = f"{PROJECT_ROOT}/sql"