"""Gold — dim_team: one row per team (SCD2-ready).

Latest name/logo per team id (by _ingested_at, then fixture date), with an
is_current flag: with only the fixtures source there is a single current
version per team — a future teams feed can add valid_from/valid_to.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.config import GOLD_DIM_TEAM, SILVER_TABLE


@dp.materialized_view(
    name=GOLD_DIM_TEAM,
    comment="Latest team name/logo per team id, is_current flag (SCD2-ready shape).",
)
def dim_team():
    silver = spark.read.table(SILVER_TABLE)

    latest_seen = Window.partitionBy("team_id").orderBy(F.desc("_ingested_at"), F.desc("date"))

    teams_src = (
        silver.select(
            F.col("home_team_id").alias("team_id"), F.col("home_team").alias("team_name"),
            F.col("home_logo").alias("team_logo"), F.col("_ingested_at"), F.col("date"))
        .unionByName(
            silver.select(
                F.col("away_team_id").alias("team_id"), F.col("away_team").alias("team_name"),
                F.col("away_logo").alias("team_logo"), F.col("_ingested_at"), F.col("date")))
    )

    return (
        teams_src
        .withColumn("rn", F.row_number().over(latest_seen))
        .where(F.col("rn") == 1)
        .drop("rn", "_ingested_at", "date")
        .withColumn("is_current", F.lit(True))
    )