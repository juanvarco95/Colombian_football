"""Pytest configuration: expose pipelines/ on sys.path and a local Spark session.

The pipeline source files under pipelines/ are the single source of truth
(shared with the notebooks via thin shims) — the tests import them directly,
exactly like GitHub Actions does.
"""

import sys
from pathlib import Path

PIPELINES_DIR = str(Path(__file__).resolve().parents[1] / "pipelines")
if PIPELINES_DIR not in sys.path:
    sys.path.insert(0, PIPELINES_DIR)

import pytest


@pytest.fixture(scope="session")
def spark():
    """Local Spark session for the builder tests (plain pyspark, no cluster)."""
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("colombian-football-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
