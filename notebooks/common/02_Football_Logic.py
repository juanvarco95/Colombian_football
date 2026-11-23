# Databricks notebook source
# DBTITLE 1,Notebook documentation
# MAGIC %md
# MAGIC
# MAGIC # Football domain logic (thin loader)
# MAGIC
# MAGIC The single source of truth is `pipelines/common/football_logic.py`, shared
# MAGIC verbatim with the Spark Declarative Pipeline and unit-tested by
# MAGIC `tests/test_football_logic.py` (pytest, runs in GitHub Actions). This notebook
# MAGIC only loads it:
# MAGIC
# MAGIC * **Scalar functions** (`match_points`, `match_result`, `tournament_from_round`,
# MAGIC   `phase_from_round`): plain Python.
# MAGIC * **Spark builders** (`points_expr`, `result_expr`, `tournament_expr`,
# MAGIC   `phase_expr`): native codegen `Column` expressions — no UDFs.
# MAGIC
# MAGIC **Run:** never run standalone — loaded via `%run ../common/02_Football_Logic`.

# COMMAND ----------

# DBTITLE 1,Load shared setup (imports + logger)
# MAGIC %run ../common/01_Setup

# COMMAND ----------

# DBTITLE 1,Thin loader for pipelines/common/football_logic.py
# ---------------------------------------------------------------------------
# Thin loader — NO logic lives here.
# Source of truth: pipelines/common/football_logic.py (scalar functions +
# Spark builders, unit-tested by tests/test_football_logic.py in CI).
# ---------------------------------------------------------------------------
import sys

PIPELINES_DIR = "/Workspace/Users/juanvarco95@gmail.com/Colombian_football/pipelines"
if PIPELINES_DIR not in sys.path:
    sys.path.append(PIPELINES_DIR)

from common.football_logic import *  # noqa: F401,F403 — scalar fns + Spark builders