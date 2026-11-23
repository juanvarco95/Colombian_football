"""Unit tests for the football domain logic (pipelines/common/football_logic.py).

The scalar functions need no Spark; the builder tests run on a local Spark
session (plain pyspark) and assert that the Spark expression builders agree
with the scalar functions on every case. Runs in GitHub Actions on every PR.
"""

from datetime import date

import pytest

from common.football_logic import (
    match_points,
    match_result,
    phase_expr,
    phase_from_round,
    points_expr,
    result_expr,
    tournament_expr,
    tournament_from_round,
)

# ---------------------------------------------------------------------------
# match_points / match_result (pure functions)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "goals_for, goals_against, points, result",
    [
        (2, 1, 3, "W"),  # narrow win
        (5, 0, 3, "W"),  # big win
        (1, 1, 1, "D"),  # draw
        (0, 0, 1, "D"),  # goalless draw
        (0, 3, 0, "L"),  # loss
        (2, 4, 0, "L"),  # heavy loss
        (4, 4, 1, "D"),  # high-scoring draw
    ],
)
def test_match_points_and_result(goals_for, goals_against, points, result):
    assert match_points(goals_for, goals_against) == points
    assert match_result(goals_for, goals_against) == result


# ---------------------------------------------------------------------------
# tournament_from_round (pure function)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "round_label, tournament",
    [
        ("Apertura - 5", "Apertura"),
        ("Apertura Final", "Apertura"),
        ("Apertura - Quadrangular - 3", "Apertura"),
        ("Clausura - 12", "Clausura"),
        ("Clausura - Quadrangular - 3", "Clausura"),
    ],
)
def test_tournament_from_round(round_label, tournament):
    assert tournament_from_round(round_label) == tournament


# ---------------------------------------------------------------------------
# phase_from_round (pure function) — body_end is the last regular fecha
# ---------------------------------------------------------------------------

BODY_END_2022_APERTURA = date(2022, 5, 16)


@pytest.mark.parametrize(
    "number_round, match_date, phase",
    [
        (0, date(2022, 6, 23), "final"),  # label 0: the two-legged final
        (1, date(2022, 5, 21), "playoff"),  # 1-6 after body end: cuadrangulares
        (1, date(2022, 1, 20), "regular"),  # 1-6 before body end: early fecha
        (7, date(2022, 5, 21), "regular"),  # 7+ is always regular
        (20, date(2022, 4, 10), "regular"),
    ],
)
def test_phase_from_round(number_round, match_date, phase):
    assert phase_from_round(number_round, match_date, BODY_END_2022_APERTURA) == phase


# ---------------------------------------------------------------------------
# Spark builders must agree with the scalar functions on every case
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "goals_for, goals_against",
    [(3, 0), (2, 1), (1, 1), (0, 0), (0, 1), (4, 4), (5, 2)],
)
def test_points_and_result_builders(spark, goals_for, goals_against):
    from pyspark.sql import functions as F

    row = (
        spark.createDataFrame([(goals_for, goals_against)], "goals_for INT, goals_against INT")
        .select(
            points_expr(F.col("goals_for"), F.col("goals_against")).cast("int").alias("points"),
            result_expr(F.col("goals_for"), F.col("goals_against")).alias("result"),
        )
        .collect()[0]
    )
    assert row.points == match_points(goals_for, goals_against)
    assert row.result == match_result(goals_for, goals_against)


@pytest.mark.parametrize(
    "round_label",
    ["Apertura - 5", "Apertura Final", "Clausura - 12", "Clausura - Quadrangular - 3"],
)
def test_tournament_builder(spark, round_label):
    from pyspark.sql import functions as F

    row = (
        spark.createDataFrame([(round_label,)], "round STRING")
        .select(tournament_expr(F.col("round")).alias("tournament"))
        .collect()[0]
    )
    assert row.tournament == tournament_from_round(round_label)


@pytest.mark.parametrize(
    "number_round, match_date",
    [
        (0, date(2022, 6, 23)),
        (1, date(2022, 5, 21)),
        (1, date(2022, 1, 20)),
        (7, date(2022, 5, 21)),
    ],
)
def test_phase_builder(spark, number_round, match_date):
    from pyspark.sql import functions as F

    row = (
        spark.createDataFrame(
            [(number_round, match_date, BODY_END_2022_APERTURA)],
            "number_round INT, match_date DATE, body_end DATE",
        )
        .select(
            phase_expr(F.col("number_round"), F.col("match_date"), F.col("body_end")).alias("phase")
        )
        .collect()[0]
    )
    assert row.phase == phase_from_round(number_round, match_date, BODY_END_2022_APERTURA)
