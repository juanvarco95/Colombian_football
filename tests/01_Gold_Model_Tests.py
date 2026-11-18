# Databricks notebook source
# DBTITLE 1,Unit tests: pure functions
# Unit tests for the pure functions in ../common/02_Football_Logic.
from datetime import date

logger = get_logger("gold_model_tests")

tests_passed = 0


def check(description: str, actual, expected) -> None:
    """Assert one expectation and log PASS lines as the tests run."""
    global tests_passed
    assert actual == expected, (
        f"FAILED: {description} — got {actual!r}, expected {expected!r}"
    )
    tests_passed += 1


# match_points / match_result
check("points: win", match_points(2, 1), 3)
check("points: big win", match_points(5, 0), 3)
check("points: draw", match_points(1, 1), 1)
check("points: goalless draw", match_points(0, 0), 1)
check("points: loss", match_points(0, 3), 0)
check("result: win", match_result(2, 1), "W")
check("result: draw", match_result(1, 1), "D")
check("result: loss", match_result(2, 4), "L")

# tournament_from_round
check("tournament: apertura fecha", tournament_from_round("Apertura - 5"), "Apertura")
check("tournament: apertura final label", tournament_from_round("Apertura Final"), "Apertura")
check("tournament: clausura fecha", tournament_from_round("Clausura - 12"), "Clausura")
check("tournament: clausura quadrangular", tournament_from_round("Clausura - Quadrangular - 3"), "Clausura")

# phase_from_round — body_end is the date of the last regular fecha
body_end_2022_apertura = date(2022, 5, 16)
check("phase: final (label 0)", phase_from_round(0, date(2022, 6, 23), body_end_2022_apertura), "final")
check("phase: playoff (label 1-6 after body end)", phase_from_round(1, date(2022, 5, 21), body_end_2022_apertura), "playoff")
check("phase: regular (label 1-6 before body end)", phase_from_round(1, date(2022, 1, 20), body_end_2022_apertura), "regular")
check("phase: regular (label 7+ after body end)", phase_from_round(7, date(2022, 5, 21), body_end_2022_apertura), "regular")

logger.info(f"Unit tests: {tests_passed} passed")

# COMMAND ----------

# DBTITLE 1,Spark builder tests: builders vs scalar functions
# The Spark builders must agree with the scalar functions on every case.
cases = [(3, 0), (2, 1), (1, 1), (0, 0), (0, 1), (4, 4), (5, 2)]

builder_df = (
    spark.createDataFrame(cases, "goals_for INT, goals_against INT")
        .withColumn(
            "points",
            points_expr(F.col("goals_for"), F.col("goals_against")).cast("int"),
        )
        .withColumn(
            "result",
            result_expr(F.col("goals_for"), F.col("goals_against")),
        )
)

mismatches = [
    (row.goals_for, row.goals_against)
    for row in builder_df.collect()
    if row.points != match_points(row.goals_for, row.goals_against)
    or row.result != match_result(row.goals_for, row.goals_against)
]
assert not mismatches, (
    f"FAILED: Spark builders disagree with scalar functions on {mismatches}"
)
logger.info(f"Spark builder tests: {len(cases)} passed (builders agree with scalar functions)")

# COMMAND ----------

# DBTITLE 1,Data tests: gold tables
# Data tests on the built gold tables (run notebooks/gold/01_Gold_Model first).
fact = spark.table(GOLD_FACT_TEAM_MATCH)
standings = spark.table(f"{GOLD_SCHEMA}.v_standings")

# Grain: exactly 2 fact rows (one per team) per match
bad_grain = fact.groupBy("match_id").count().where(F.col("count") != 2).count()
assert bad_grain == 0, f"FAILED: {bad_grain} matches without exactly 2 fact rows"
tests_passed += 1

# points / result consistent with the goals columns
bad_points = (
    fact
        .withColumn(
            "expected_points",
            points_expr(F.col("goals_for"), F.col("goals_against")).cast("int"),
        )
        .withColumn(
            "expected_result",
            result_expr(F.col("goals_for"), F.col("goals_against")),
        )
        .where(
            (F.col("points") != F.col("expected_points"))
            | (F.col("result") != F.col("expected_result"))
        )
        .count()
)
assert bad_points == 0, f"FAILED: {bad_points} fact rows with wrong points/result"
tests_passed += 1

# Standings shape: 20 teams per season-tournament, points arithmetic holds
bad_standings = (
    standings
        .groupBy("season", "tournament")
        .agg(
            F.countDistinct("team_name").alias("teams"),
            F.sum("points").alias("points_sum"),
            (3 * F.sum("won") + F.sum("drawn")).alias("points_expected"),
        )
        .where((F.col("teams") != 20) | (F.col("points_sum") != F.col("points_expected")))
        .count()
)
assert bad_standings == 0, f"FAILED: {bad_standings} season-tournaments with broken standings"
tests_passed += 1

# dim_team covers every team in the fact
missing_teams = (
    fact.select("team_id").distinct()
        .join(spark.table(GOLD_DIM_TEAM), "team_id", "left_anti")
        .count()
)
assert missing_teams == 0, f"FAILED: {missing_teams} fact teams missing from dim_team"
tests_passed += 1

# dim_date covers every match date
missing_dates = (
    fact.select(F.date_format("match_date", "yyyyMMdd").cast("int").alias("date_key"))
        .distinct()
        .join(spark.table(GOLD_DIM_DATE), "date_key", "left_anti")
        .count()
)
assert missing_dates == 0, f"FAILED: {missing_dates} match dates missing from dim_date"
tests_passed += 1

logger.info("Data tests: 5 passed (grain, points, standings shape, dims coverage)")

# COMMAND ----------

# DBTITLE 1,Summary
logger.info(
    f"Test run completed | {tests_passed} unit + data checks passed, "
    "plus 7 Spark builder checks"
)

# COMMAND ----------

# DBTITLE 1,Notebook documentation
# MAGIC %md
# MAGIC
# MAGIC # Gold model tests
# MAGIC
# MAGIC Test suite for the gold layer:
# MAGIC
# MAGIC 1. **Unit tests** for the pure functions in `common/02_Football_Logic`
# MAGIC    (points, result, tournament, phase classification).
# MAGIC 2. **Builder tests**: the Spark expression builders must agree with the scalar
# MAGIC    functions on every case.
# MAGIC 3. **Data tests** on the built gold tables: fact grain (exactly 2 rows per
# MAGIC    match), points/result consistency, standings shape, dimension coverage.
# MAGIC
# MAGIC Every check is a hard `assert` — a failure stops the run with a clear message.
# MAGIC
# MAGIC **Run:** after `notebooks/gold/01_Gold_Model` has run at least once.
# MAGIC (Future: port to pytest + GitHub Actions CI per the project spec, Fase 6.)

# COMMAND ----------

# DBTITLE 1,Load shared setup (imports + logger)
# MAGIC %run ../common/01_Setup

# COMMAND ----------

# DBTITLE 1,Load pipeline configuration (constants)
# MAGIC %run ../common/02_Config

# COMMAND ----------

# DBTITLE 1,Load football domain logic (functions)
# MAGIC %run ../common/02_Football_Logic

# COMMAND ----------

