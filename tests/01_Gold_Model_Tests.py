# Databricks notebook source
# DBTITLE 1,Notebook documentation
# MAGIC %md
# MAGIC
# MAGIC # Gold model tests (data tests)
# MAGIC
# MAGIC Data tests on the gold tables built by the Spark Declarative Pipeline
# MAGIC (`pipelines/`): fact grain (exactly 2 rows per match), points/result
# MAGIC consistency, standings shape, SCD2 dim_team validity/coverage, dim_date
# MAGIC coverage.
# MAGIC
# MAGIC **Unit + builder tests moved to pytest:** `tests/test_football_logic.py`
# MAGIC runs in GitHub Actions on every PR (pure Python + a local Spark session —
# MAGIC no workspace needed). This notebook keeps only the checks that need the
# MAGIC built tables.
# MAGIC
# MAGIC Every check is a hard `assert` — a failure stops the run with a clear message.
# MAGIC
# MAGIC **Run:** after a pipeline update has completed at least once.

# COMMAND ----------

# DBTITLE 1,Load shared setup (imports + logger)
# MAGIC %run ../notebooks/common/01_Setup

# COMMAND ----------

# DBTITLE 1,Load pipeline configuration (constants)
# MAGIC %run ../notebooks/common/02_Config

# COMMAND ----------

# DBTITLE 1,Load football domain logic (functions)
# MAGIC %run ../notebooks/common/02_Football_Logic

# COMMAND ----------

# DBTITLE 1,Test setup (unit tests moved to pytest)
# Unit + builder tests live in pytest now: tests/test_football_logic.py
# (runs in GitHub Actions — see .github/workflows/cicd.yml).
# This notebook keeps the WORKSPACE data tests on the built gold tables.
logger = get_logger("gold_model_tests")
tests_passed = 0

# COMMAND ----------

# DBTITLE 1,Data tests: gold tables
# Data tests on the gold tables built by the SDP pipeline (run an update first).
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

# dim_team is SCD2: exactly one current version per team (__END_AT IS NULL)
dim_current = spark.table(GOLD_DIM_TEAM).where(F.col("__END_AT").isNull())
dup_current = dim_current.groupBy("team_id").count().where(F.col("count") != 1).count()
assert dup_current == 0, f"FAILED: {dup_current} teams without exactly one current dim_team row"
tests_passed += 1

# current versions are well-formed SCD2 rows (validity window open, __START_AT set)
bad_validity = dim_current.where(F.col("__START_AT").isNull()).count()
assert bad_validity == 0, f"FAILED: {bad_validity} current dim_team rows without __START_AT"
tests_passed += 1

# the current team versions cover every team in the fact
missing_teams = (
    fact.select("team_id").distinct()
        .join(dim_current, "team_id", "left_anti")
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

logger.info("Data tests: 7 passed (grain, points, standings shape, SCD2 dim_team, dims coverage)")

# COMMAND ----------

# DBTITLE 1,Summary
logger.info(
    f"Test run completed | {tests_passed} data checks passed "
    "(unit + builder tests live in tests/test_football_logic.py — pytest/CI)"
)