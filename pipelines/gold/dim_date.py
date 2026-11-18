"""Gold — dim_date: one row per day covering the full match date range.

Built with sequence() + explode() over the min/max match date of the fact
table — a pure DataFrame date spine (no SQL string, no collect).
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.config import GOLD_DIM_DATE, GOLD_FACT_TEAM_MATCH


@dp.materialized_view(
    name=GOLD_DIM_DATE,
    comment="One row per day over the full match date range, with calendar attributes.",
)
def dim_date():
    fact = spark.read.table(GOLD_FACT_TEAM_MATCH)

    bounds = fact.agg(
        F.min("match_date").alias("first_day"),
        F.max("match_date").alias("last_day"),
    )

    return (
        bounds
        .select(F.explode(F.sequence(F.col("first_day"), F.col("last_day"))).alias("full_date"))
        .select(
            F.date_format(F.col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
            F.col("full_date"),
            F.year(F.col("full_date")).alias("year"),
            F.quarter(F.col("full_date")).alias("quarter"),
            F.month(F.col("full_date")).alias("month"),
            F.date_format(F.col("full_date"), "MMMM").alias("month_name"),
            F.dayofmonth(F.col("full_date")).alias("day"),
            F.dayofweek(F.col("full_date")).alias("day_of_week"),
            F.date_format(F.col("full_date"), "EEEE").alias("day_name"),
            F.dayofweek(F.col("full_date")).isin(1, 7).alias("is_weekend"),
            F.weekofyear(F.col("full_date")).alias("week_of_year"),
        )
    )