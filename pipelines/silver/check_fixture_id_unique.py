"""Data quality check — fixture id is unique in silver (aggregate invariant).

A private materialized view that materializes only violations (fixture ids
ingested more than once after the dedup window); the expectation fails the
check flow when any appears. Uniqueness cannot be a row-level expectation,
which is why it lives in this aggregate check dataset.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.config import SILVER_TABLE


@dp.materialized_view(private=True, refresh_policy="incremental")
@dp.expect_or_fail("fixture id unique in silver", "cnt = 1")
def check_fixture_id_unique():
    return (
        spark.read.table(SILVER_TABLE)
        .groupBy("id")
        .agg(F.count("*").alias("cnt"))
        .where(F.col("cnt") > 1)
    )