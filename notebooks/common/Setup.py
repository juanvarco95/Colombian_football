# Databricks notebook source
# DBTITLE 1,Notebook documentation
# MAGIC %md
# MAGIC # Setup (shared imports + logger)
# MAGIC
# MAGIC Single source of truth for the Colombian football pipeline notebooks:
# MAGIC
# MAGIC * **Common imports** — standard library, `requests`, PySpark helpers (`F`, `StructType`, `Window`, …), and `DeltaTable` from `delta.tables` for Delta MERGE operations
# MAGIC * **Logger factory** — `get_logger("<notebook>")` so every pipeline run leaves a trace in the cell output
# MAGIC
# MAGIC Every notebook under `notebooks/` loads this via `%run ../common/Setup` (or the absolute path) as its first runnable cell.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Shared imports and logger factory
import json
import logging
import os
import sys
from datetime import datetime, timezone

import requests
import pyspark.sql.functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def get_logger(notebook: str) -> logging.Logger:
    """Return a logger tagged with the calling notebook's name.

    Writes to stdout so every run's messages are visible in cell output.
    """
    logger = logging.getLogger(f"football_col.{notebook}")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
    logger.propagate = False
    return logger


logger = get_logger("setup")
logger.info("Shared setup loaded: imports and logger are ready.")


# COMMAND ----------

