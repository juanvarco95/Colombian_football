"""Gold — dim_team: SCD Type 2 team dimension (AUTO CDC from snapshot).

Teams change names and crests over time, so history matters. Every update,
the current team snapshot (one row per team id, extracted from silver) is
diffed against the previous one by AUTO CDC FROM SNAPSHOT: a changed name or
crest closes the old version (__END_AT) and opens a new one (__START_AT).

Current row per team: `__END_AT IS NULL`. Point-in-time lookups:
`__START_AT <= t AND (__END_AT > t OR __END_AT IS NULL)`.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.config import GOLD_DIM_TEAM, SILVER_TABLE


@dp.temporary_view(name="dim_team_snapshot")
@dp.expect_or_fail("team id present", "team_id IS NOT NULL")
def dim_team_snapshot():
    """One row per team id — latest name/logo seen in the silver fixtures."""
    silver = spark.read.table(SILVER_TABLE)

    latest_seen = Window.partitionBy("team_id").orderBy(F.desc("_ingested_at"))

    home = silver.select(
        F.col("home_team_id").alias("team_id"),
        F.col("home_team").alias("team_name"),
        F.col("home_logo").alias("team_logo"),
        F.col("_ingested_at"),
    )
    away = silver.select(
        F.col("away_team_id").alias("team_id"),
        F.col("away_team").alias("team_name"),
        F.col("away_logo").alias("team_logo"),
        F.col("_ingested_at"),
    )

    return (
        home.unionByName(away)
        .withColumn("rn", F.row_number().over(latest_seen))
        .where(F.col("rn") == 1)
        .drop("rn", "_ingested_at")
    )


# Target: a streaming table owned by this pipeline. NOTE: this changed the
# dataset type (it was a materialized view) — the old gold.dim_team table had
# to be dropped before the first update after this change.
dp.create_streaming_table(
    name=GOLD_DIM_TEAM,
    comment=(
        "Team dimension, SCD Type 2 via AUTO CDC from snapshot: __START_AT/__END_AT "
        "validity; the current version of each team has __END_AT IS NULL."
    ),
)

dp.create_auto_cdc_from_snapshot_flow(
    target=GOLD_DIM_TEAM,
    source="dim_team_snapshot",
    keys=["team_id"],
    stored_as_scd_type=2,  # full history: name/crest changes create new versions
)