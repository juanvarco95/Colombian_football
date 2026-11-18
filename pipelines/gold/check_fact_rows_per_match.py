"""Data quality check — exactly 2 fact rows per match (aggregate invariant).

A private materialized view that materializes only violations (matches whose
team-per-match grain produced a row count other than 2); the expectation
fails the check flow when any appears.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.config import GOLD_FACT_TEAM_MATCH


@dp.materialized_view(private=True, refresh_policy="incremental")
@dp.expect_or_fail("exactly two fact rows per match", "cnt = 2")
def check_fact_rows_per_match():
    return (
        spark.read.table(GOLD_FACT_TEAM_MATCH)
        .groupBy("match_id")
        .agg(F.count("*").alias("cnt"))
        .where(F.col("cnt") != 2)
    )