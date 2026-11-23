# Databricks notebook source
# DBTITLE 1,Notebook documentation
# MAGIC %md
# MAGIC
# MAGIC # Pipeline configuration (thin loader)
# MAGIC
# MAGIC The single source of truth for every name in the project is
# MAGIC `pipelines/common/config.py` — git-tracked, unit-tested, and shared verbatim
# MAGIC with the Spark Declarative Pipeline. This notebook only loads that module, so
# MAGIC notebooks and pipeline can never drift apart.
# MAGIC
# MAGIC * No constant lives here. `%run ../common/02_Config` still defines every name
# MAGIC   (`CATALOG`, table names, volume paths, API settings) in the caller's
# MAGIC   namespace — existing notebooks keep working unchanged.
# MAGIC * The `catalog` widget selects which catalog the names resolve to: the
# MAGIC   bundle's job sets it to `football_col_prod` for the prod target; the
# MAGIC   default is `football_col_dev`.

# COMMAND ----------

# DBTITLE 1,Load shared setup (imports + logger)
# MAGIC %run ../common/01_Setup

# COMMAND ----------

# DBTITLE 1,Configuration constants (single source of truth)
# ---------------------------------------------------------------------------
# Thin loader — NO constants live here.
# Source of truth: pipelines/common/config.py (shared with the pipeline;
# catalog resolution: FOOTBALL_CATALOG env var > football.catalog pipeline
# config > football_col_dev default).
# ---------------------------------------------------------------------------
import sys

PIPELINES_DIR = "/Workspace/Users/juanvarco95@gmail.com/Colombian_football/pipelines"
if PIPELINES_DIR not in sys.path:
    sys.path.append(PIPELINES_DIR)

from common import config as _config

# Job/notebook parameter: which catalog this run targets (dev by default).
dbutils.widgets.text("catalog", _config.resolve_catalog())
_catalog = dbutils.widgets.get("catalog").strip() or _config.DEFAULT_CATALOG

# Re-export every constant into this notebook's namespace for %run consumers
_ns = _config.for_catalog(_catalog)
globals().update(vars(_ns))
globals().update({
    name: getattr(_config, name)
    for name in (
        "API_BASE_URL", "LEAGUE_ID", "SECRET_SCOPE", "SECRET_KEY",
        "SECONDS_BETWEEN_CALLS", "RATE_LIMIT_WAIT", "PROJECT_ROOT", "SQL_DIR",
    )
})