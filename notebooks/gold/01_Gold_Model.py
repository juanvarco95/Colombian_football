# Databricks notebook source
# DBTITLE 1,Notebook documentation
# MAGIC %md
# MAGIC # Gold Model (Dimensional Layer)
# MAGIC
# MAGIC Builds the dimensional Gold layer from silver: a star schema with a team-match fact table and two dimensions, plus business-ready views for standings and points progression.
# MAGIC
# MAGIC * **Source:** `football_col_dev.silver.api_fixtures` (clean, deduplicated, one row per fixture)
# MAGIC * **Target:** `football_col_dev.gold` — `fact_team_match`, `dim_team`, `dim_date`, views `v_standings` and `v_points_progression`
# MAGIC
# MAGIC **How it works**
# MAGIC 1. Loads shared imports and the run logger via `%run ../common/01_Setup`.
# MAGIC 2. **Phase classification:** the API round labels 1–6 mix the early *fechas* with the end-of-semester *cuadrangulares* (top-8 playoff, 6 matchdays), which the API also labels 1–6. The playoff fixtures are the ones played **after the last body fecha** (max match date of round labels 7+). Every fixture gets `phase` = `regular` (the all-play-all fechas), `playoff` (cuadrangulares), or `final` (2-leg final, `number_round` = 0). Verified against the data: every team plays exactly 20 regular games (19 in 2024), the top 8 play exactly 6 playoff games, finalists 2.
# MAGIC 3. **`fact_team_match`** — grain: one row per **team per played match** (each match generates 2 rows, home + away perspective), with `points` (3/1/0) and `result` (W/D/L). With this grain the standings are a simple GROUP BY.
# MAGIC 4. **`dim_team`** — one row per team (latest name/logo seen in silver), `is_current` = true (SCD2-ready shape).
# MAGIC 5. **`dim_date`** — one row per day covering all match dates, built with `sequence()` + `explode()`.
# MAGIC 6. **Views:** `v_standings` (regular phase only, ranked by points, goal difference, goals for) and `v_points_progression` (cumulative points per team per matchday). All SQL DDL and validation scripts live as standalone `.sql` files in the project's `sql/` folder (`01_create_gold_schema.sql` … `05_standings_leaders.sql`) and are executed by this notebook via `run_sql_script()`, so they can also be run directly in the SQL editor.
# MAGIC
# MAGIC **Run:** top to bottom, after the Silver notebook. Idempotent: tables are rebuilt (overwrite) and views replaced on every run.

# COMMAND ----------

# DBTITLE 1,Load pipeline configuration (constants)
# MAGIC %run ../common/02_Config

# COMMAND ----------

# DBTITLE 1,Load football domain logic (functions)
# MAGIC %run ../common/02_Football_Logic

# COMMAND ----------

# DBTITLE 1,Load shared setup (imports + logger)
# MAGIC %run ../common/01_Setup

# COMMAND ----------

# DBTITLE 1,Config (from 02_Config), gold schema and phase classification
# Run logger from ../common/01_Setup (loaded via %run), tagged to this notebook
logger = get_logger("gold_model")

# Configuration comes from ../common/02_Config: SILVER_TABLE, GOLD_SCHEMA, SQL_DIR.
# Football derivation logic comes from ../common/02_Football_Logic.
source_table = SILVER_TABLE
gold_schema = GOLD_SCHEMA


def run_sql_script(name: str):
    """Execute one statement from a .sql file in the project's sql/ folder."""
    with open(f"{SQL_DIR}/{name}") as f:
        return spark.sql(f.read())


# Create the gold schema (idempotent) — sql/01_create_gold_schema.sql
run_sql_script("01_create_gold_schema.sql")

logger.info(f"Gold model run started | source: {source_table} | target schema: {gold_schema}")

silver = spark.read.table(source_table)
logger.info(f"Loaded {silver.count()} fixtures from {source_table}")

# ---- Phase classification (logic in ../common/02_Football_Logic) ------------
# Round labels 1-6 mix the early fechas with the cuadrangulares (top-8 playoff,
# 6 matchdays at the end of the semester), which the API also labels 1-6.
# The playoff fixtures are those played AFTER the last body fecha
# (max match date over round labels 7+), so each tournament splits cleanly into:
#   regular  = the all-play-all fechas (20 games per team in 2022-23, 19 in 2024)
#   playoff  = cuadrangulares (6 games for each of the top 8 teams)
#   final    = the 2-leg final (number_round = 0)
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

phase_counts = fixtures.groupBy("season", "tournament", "phase").count().orderBy("season", "tournament", "phase")
logger.info("Phase split per season/tournament:\n" + phase_counts.toPandas().to_string(index=False))

# COMMAND ----------

# DBTITLE 1,Build fact_team_match (team per match grain)
# ---- fact_team_match: one row per team per played match -------------------
# Only finished matches (FT = full time, PEN = decided on penalties).
played = fixtures.where(F.col("status").isin("FT", "PEN"))

def team_side(is_home: bool):
    """Project a match from one team's perspective (home or away)."""
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

fact_team_match = (
    team_side(True)
        .unionByName(team_side(False))
        .withColumns({
            "points": points_expr(F.col("goals_for"), F.col("goals_against")).cast("int"),
            "result": result_expr(F.col("goals_for"), F.col("goals_against")),
            "date_key": F.date_format(F.col("match_date"), "yyyyMMdd").cast("int"),
        })
)

fact_count = fact_team_match.count()
(
    fact_team_match.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_FACT_TEAM_MATCH)
)
logger.info(f"fact_team_match rebuilt | {fact_count} rows (2 per played match, {fact_count // 2} matches)")

# COMMAND ----------

# DBTITLE 1,Build dim_team and dim_date
# ---- dim_team: latest name/logo per team id ---------------------------------
# SCD2-ready shape (is_current); with only the fixtures source there is a single
# current version per team — a future teams feed can add valid_from/valid_to.
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

dim_team = (
    teams_src
        .withColumn("rn", F.row_number().over(latest_seen))
        .where(F.col("rn") == 1)
        .drop("rn", "_ingested_at", "date")
        .withColumn("is_current", F.lit(True))
)

dim_team_count = dim_team.count()
(
    dim_team.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_DIM_TEAM)
)
logger.info(f"dim_team rebuilt | {dim_team_count} teams")

# ---- dim_date: one row per day over the match date range (sequence + explode) -
first_day, last_day = played.agg(
    F.min("match_date").alias("mn"), F.max("match_date").alias("mx")
).collect()[0]

# Pure PySpark date spine: sequence() builds the day array, explode() one row per day
# (no spark.sql string — avoids the f-string quoting that broke the old cell).
dim_date = (
    spark.range(1)
        .select(F.explode(F.sequence(F.lit(first_day), F.lit(last_day))).alias("full_date"))
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

dim_date_count = dim_date.count()
(
    dim_date.write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(GOLD_DIM_DATE)
)
logger.info(f"dim_date rebuilt | {dim_date_count} days ({first_day} .. {last_day})")

# COMMAND ----------

# DBTITLE 1,Create v_standings and v_points_progression (sql/)
# ---- Views for dashboards (logic lives in Gold, not the dashboard) -----------

# Standings: regular phase only (the all-play-all fechas), ranked by points,
# goal difference, goals for — mirroring the official Dimayor tie-breakers.
# DDL lives in sql/02_create_v_standings.sql
run_sql_script("02_create_v_standings.sql")

# Points progression: cumulative points per team per matchday (regular phase),
# one line per team in a dashboard chart. DDL in sql/03_create_v_points_progression.sql
run_sql_script("03_create_v_points_progression.sql")

logger.info(f"Views created: {gold_schema}.v_standings, {gold_schema}.v_points_progression")

# COMMAND ----------

# DBTITLE 1,Validation checks (sql/)
# Integrity checks: grain, arithmetic, standings shape — sql/04_validate_gold_integrity.sql
# Every check must return 0 violations.
display(run_sql_script("04_validate_gold_integrity.sql"))

# COMMAND ----------

# DBTITLE 1,Standings leaders check (sql/)
# Regular-phase leaders per season and tournament (sanity check vs public records)
# — sql/05_standings_leaders.sql
display(run_sql_script("05_standings_leaders.sql"))

# COMMAND ----------

# DBTITLE 1,Run completed
logger.info(f"Gold model run completed | {gold_schema}: fact_team_match, dim_team, dim_date, v_standings, v_points_progression")

# COMMAND ----------

