# Databricks notebook source
# DBTITLE 1,Notebook documentation
# MAGIC %md
# MAGIC
# MAGIC # Football domain logic
# MAGIC
# MAGIC Pure Python functions and their Spark-native expression builders for the
# MAGIC Colombian Primera A pipeline. Two layers, always kept in sync:
# MAGIC
# MAGIC * **Scalar functions** (`match_points`, `match_result`, `tournament_from_round`,
# MAGIC   `phase_from_round`): plain Python, unit-testable in `tests/`.
# MAGIC * **Spark builders** (`points_expr`, `result_expr`, `tournament_expr`,
# MAGIC   `phase_expr`): return `Column` expressions for native Spark codegen — no UDFs.
# MAGIC
# MAGIC The gold notebook builds `fact_team_match` with the builders; the tests
# MAGIC notebook asserts that both layers always agree.
# MAGIC
# MAGIC **Run:** never run standalone — loaded via `%run ../common/02_Football_Logic`.

# COMMAND ----------

# DBTITLE 1,Load shared setup (imports + logger)
# MAGIC %run ../common/01_Setup

# COMMAND ----------

# DBTITLE 1,Scalar functions (pure Python)
# ---------------------------------------------------------------------------
# Scalar functions (pure Python, unit-testable)
# ---------------------------------------------------------------------------

def tournament_from_round(round_label: str) -> str:
    """Return the semester tournament ('Apertura' or 'Clausura') of a round label.

    API labels look like 'Apertura - 5' or 'Clausura - Quadrangular - 3'.
    """
    return "Apertura" if round_label.startswith("Apertura") else "Clausura"


def phase_from_round(number_round: int, match_date, body_end) -> str:
    """Classify a fixture as 'regular', 'playoff' or 'final'.

    The API reuses round labels 1-6 for the end-of-semester cuadrangulares
    (top-8 playoff, 6 matchdays). A fixture with label 0 is the two-legged
    final; labels 1-6 played after the last regular fecha (body_end, the max
    match date of round labels 7+) are playoff games; everything else is
    regular phase.
    """
    if number_round == 0:
        return "final"
    if 1 <= number_round <= 6 and match_date > body_end:
        return "playoff"
    return "regular"


def match_points(goals_for: int, goals_against: int) -> int:
    """Points earned from one match: 3 win, 1 draw, 0 loss."""
    if goals_for > goals_against:
        return 3
    if goals_for == goals_against:
        return 1
    return 0


def match_result(goals_for: int, goals_against: int) -> str:
    """Match outcome from the team's perspective: 'W', 'D' or 'L'."""
    if goals_for > goals_against:
        return "W"
    if goals_for == goals_against:
        return "D"
    return "L"

# COMMAND ----------

# DBTITLE 1,Spark expression builders
from pyspark.sql import Column


# ---------------------------------------------------------------------------
# Spark builders (native codegen, no UDFs — mirror the scalar functions above)
# ---------------------------------------------------------------------------

def points_expr(goals_for: Column, goals_against: Column) -> Column:
    """Column equivalent of match_points()."""
    return (
        F.when(goals_for > goals_against, F.lit(3))
        .when(goals_for == goals_against, F.lit(1))
        .otherwise(F.lit(0))
    )


def result_expr(goals_for: Column, goals_against: Column) -> Column:
    """Column equivalent of match_result()."""
    return (
        F.when(goals_for > goals_against, F.lit("W"))
        .when(goals_for == goals_against, F.lit("D"))
        .otherwise(F.lit("L"))
    )


def tournament_expr(round_label: Column) -> Column:
    """Column equivalent of tournament_from_round()."""
    return (
        F.when(round_label.startswith("Apertura"), F.lit("Apertura"))
        .otherwise(F.lit("Clausura"))
    )


def phase_expr(number_round: Column, match_date: Column, body_end: Column) -> Column:
    """Column equivalent of phase_from_round()."""
    return (
        F.when(number_round == 0, F.lit("final"))
        .when(
            number_round.between(1, 6) & (match_date > body_end),
            F.lit("playoff"),
        )
        .otherwise(F.lit("regular"))
    )