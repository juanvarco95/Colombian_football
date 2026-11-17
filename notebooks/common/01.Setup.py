# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
"""
Shared setup for the Colombian football pipeline.

Every notebook under notebooks/ starts its first runnable cell with:

    %run ../common/01.Setup

Single source of truth for:
  * all common imports (standard library, requests, PySpark helpers)
  * a configured logger so every pipeline run leaves a trace
"""

# COMMAND ----------

# DBTITLE 1,Shared setup (imports + logger)


import json
import logging
import os
import sys
from datetime import datetime, timezone

import requests
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)



# COMMAND ----------


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