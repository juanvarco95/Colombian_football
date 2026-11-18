"""Gold — fact_team_match: one row per team per played match.

Grain: each finished match produces 2 rows (home + away perspective) with
derived points (3/1/0), result (W/D/L) and date_key. Only finished matches
(FT/PEN in the validated data) enter the fact. Expectations enforce the
Fase 4 gold invariants: points consistent with goals, phase valid, goals
present.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.config import GOLD_FACT_TEAM_MATCH, SILVER_TABLE
from common.football_logic import phase_expr, points_expr, result_expr, tournament_expr


def _team_side(played, is_home):
    """Project a played match from one team's perspective (home or away)."""
    own, opp = ("home", "away") if is_home else ("away", "home")
    return played.select(
        F.col("id").alias("match_id"),
        F.col("season").cast("int").alias("season"),
        F.col("tournament"),
        F.col("phase"),
        F.col("number_round").cast("int").alias("number_round"),
        F.col("match_date"),
        F.col(f"{own}_team_id").alias("team_id"),
        F.col(f"{opp}_team_id").alias("opponent_id"),
        F.lit(is_home).alias("is_home"),
        F.col(f"{own}_goals").cast("int").alias("goals_for"),
        F.col(f"{opp}_goals").cast("int").alias("goals_against"),
    )


@dp.materialized_view(
    name=GOLD_FACT_TEAM_MATCH,
    comment=(
        "One row per team per played match (home + away perspective), with "
        "points/result/date_key derived; only finished matches."
    ),
)
@dp.expect_or_fail(
    "points in (0, 1, 3) consistent with goals",
    "points IN (0, 1, 3) AND points = CASE WHEN goals_for > goals_against THEN 3 "
    "WHEN goals_for = goals_against THEN 1 ELSE 0 END",
)
@dp.expect_or_fail("phase is regular, playoff or final", "phase IN ('regular', 'playoff', 'final')")
@dp.expect_or_fail("goals present", "goals_for IS NOT NULL AND goals_against IS NOT NULL")
def fact_team_match():
    silver = spark.read.table(SILVER_TABLE)

    # Phase classification: round labels 1-6 mix the early fechas with the
    # cuadrangulares (top-8 playoff, 6 matchdays at the end of the semester,
    # also labeled 1-6). Playoff fixtures are those played AFTER the last body
    # fecha (max match date over round labels 7+):
    #   regular = the all-play-all fechas (20 games per team in 2022-23, 19 in 2024)
    #   playoff = cuadrangulares (6 games for each of the top 8 teams)
    #   final   = the 2-leg final (number_round = 0)
    body_end = (
        silver
        .where(F.col("number_round") >= 7)
        .withColumn("tournament", tournament_expr(F.col("round")))
        .groupBy("season", "tournament")
        .agg(F.max(F.to_date(F.col("date"), "dd-MM-yyyy")).alias("body_end"))
    )

    fixtures = (
        silver
        .withColumn("tournament", tournament_expr(F.col("round")))
        .withColumn("match_date", F.to_date(F.col("date"), "dd-MM-yyyy"))
        .join(body_end, on=["season", "tournament"])
        .withColumn(
            "phase",
            phase_expr(F.col("number_round"), F.col("match_date"), F.col("body_end")),
        )
    )

    played = fixtures.where(F.col("status").isin("FT", "PEN"))

    return (
        _team_side(played, True)
        .unionByName(_team_side(played, False))
        .withColumns({
            "points": points_expr(F.col("goals_for"), F.col("goals_against")).cast("int"),
            "result": result_expr(F.col("goals_for"), F.col("goals_against")),
            "date_key": F.date_format(F.col("match_date"), "yyyyMMdd").cast("int"),
        })
    )