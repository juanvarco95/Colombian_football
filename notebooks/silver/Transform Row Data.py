# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Notebook documentation
# MAGIC %md
# MAGIC # Transform Row Data (Silver)
# MAGIC
# MAGIC Flattens the raw JSON API response stored in bronze into a clean, tabular silver table — one row per fixture.
# MAGIC
# MAGIC * **Source:** `football_col_dev.bronze.api_fixtures` (raw JSON from API-Football, ingested by the AutoLoader notebook)
# MAGIC * **Target:** `football_col_dev.silver.api_fixtures` (MERGE — inserts fixtures not yet present, keyed by `id`)
# MAGIC
# MAGIC **How it works**
# MAGIC 1. Loads shared imports and the run logger from `../common/setup.py` (imported as a module).
# MAGIC 2. Loads each schema from its own notebook in `../Schemas/` (`response_schema`, `parameters_schema`, `bronze_explode_schema`) via `%run`.
# MAGIC 3. Parses the `response` / `parameters` JSON columns with `from_json` + `explode` and flattens them into one row per fixture.
# MAGIC 4. Deduplicates to the latest ingest per fixture id (window `row_number` over `_ingested_at`), derives `round` / `number_round` from the API round label (null-safe `try_cast`), uses null-safe `try_cast` for the date, and enforces the explicit 22-column output schema (21 fixture columns incl. `number_round` + `_ingested_at`) with `.to(bronze_explode_schema)`.
# MAGIC 5. Merges into silver keyed by `id`: inserts fixtures not yet present, and updates an existing row whenever the derived values differ (NULL-safe `IS DISTINCT FROM` over all columns) — so both newer ingests and transform-logic changes propagate.
# MAGIC
# MAGIC **Run:** top to bottom, after the AutoLoader notebook. Every step logs progress, row counts, and inserted rows.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load shared setup (imports + logger)
import os
import sys

# The notebook's working directory is its own folder, so this resolves in both
# the Git folder and the bundle deploy path.
sys.path.append(os.path.abspath("../common"))
from setup import *  # shared imports (F, requests, DeltaTable, ...) + get_logger

# COMMAND ----------

# DBTITLE 1,Load response schema
# MAGIC %run ../Schemas/response_schema

# COMMAND ----------

# DBTITLE 1,Load parameters schema
# MAGIC %run ../Schemas/parameters_schema

# COMMAND ----------

# DBTITLE 1,Load bronze explode schema
# MAGIC %run ../Schemas/bronze_explode_schema

# COMMAND ----------

# DBTITLE 1,Read bronze source table
# Run logger from ../common/setup.py, tagged to this notebook
logger = get_logger("transform_row_data")

source_table = 'football_col_dev.bronze.api_fixtures'
target_table = 'football_col_dev.silver.api_fixtures'

logger.info(f"Silver transform run started | source: {source_table} | target: {target_table}")

# Raw bronze rows: one row per landed JSON file, plus ingestion metadata columns
bronze_df = spark.read.table(source_table)
logger.info(f"Loaded {bronze_df.count()} rows from {source_table}")

# COMMAND ----------

# DBTITLE 1,Extract and flatten fixture columns
# Bronze is append-only: the same fixture can arrive in several landed files.
# Rank rows per fixture id by ingestion time and keep only the latest ingest.
latest = Window.partitionBy("id").orderBy(F.desc("_ingested_at"))

bronze_explode_df = (
    bronze_df
        .select(
            # ingestion metadata — ranks duplicates for dedup, and is kept in the output
            # so the silver MERGE can update a row when a newer ingest arrives
            F.col("_ingested_at"),
            # explode the JSON response array into one row per fixture, typed by the DDL schemas from ../Schemas/
            F.explode(F.from_json(F.col("response"), response_schema)).alias("response"),
            F.from_json(F.col("parameters"), parameters_schema).alias("parameters"),
        )
        .withColumns({
            'id':      F.col('response.fixture.id'),
            'season':  F.col('parameters.season'),
            # try_cast: null-safe date parsing (a malformed API value yields NULL instead of failing the run)
            'date':    F.date_format(F.expr("try_cast(response.fixture.date AS DATE)"), 'dd-MM-yyyy'),
            'league':  F.col('response.league.name'),
            'round':   F.col('response.league.round'),
            'referee': F.col('response.fixture.referee'),
            'city':    F.col('response.fixture.venue.city'),
            'stadium': F.col('response.fixture.venue.name'),
            'home_team': F.col('response.teams.home.name'),
            'away_team': F.col('response.teams.away.name'),
            'home_logo': F.col('response.teams.home.logo'),
            'away_logo': F.col('response.teams.away.logo'),
            'home_goals': F.col('response.goals.home'),
            'away_goals': F.col('response.goals.away'),
            'home_halftime_goals': F.col('response.score.halftime.home'),
            'away_halftime_goals': F.col('response.score.halftime.away'),
            'home_penalty_goals': F.col('response.score.penalty.home'),
            'away_penalty_goals': F.col('response.score.penalty.away'),
            'home_winner': F.col('response.teams.home.winner'),
            'away_winner': F.col('response.teams.away.winner'),
        })
        # keep only the latest ingest per fixture id
        .withColumn('rn', F.row_number().over(latest))
        # split the API round label into phase and number, e.g. 'Apertura - 1' -> ('Apertura', 1)
        # element_at(-1) takes the LAST part, so 'Apertura - Quadrangular - 1' still yields 1
        .withColumn('round_parts', F.split(F.col('round'), '-'))
        .withColumn('round', F.trim(F.col('round_parts').getItem(0)))
        # try_cast: unnumbered rounds (e.g. 'Clausura - Final') become NULL instead of failing
        .withColumn('number_round', F.expr("try_cast(trim(element_at(round_parts, -1)) AS BIGINT)"))
        .withColumn('round', F.when(
            F.col('number_round').isNull(),
            F.concat(F.col('round'), F.lit(' '), F.lit('Final'))
        ).otherwise(F.col('round')))
        .withColumn('number_round', F.when(
            F.col('number_round').isNull(),
            F.lit(0)
        ).otherwise(F.col('number_round')))
        .filter(F.col('rn') == 1)
        .drop('response', 'parameters', 'rn', 'round_parts')
        # enforce the explicit 22-column output schema (21 fixture columns incl. number_round + _ingested_at) from ../Schemas/bronze_explode_schema
        .to(bronze_explode_schema)
)


logger.info(f"Flattened {bronze_explode_df.count()} fixtures (deduplicated to the latest ingest per fixture id)")

# COMMAND ----------

# Register the flattened fixtures as a temp view for ad-hoc SQL exploration
bronze_explode_df.createOrReplaceTempView("bronze_explode_df")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_explode_df
# MAGIC where number_round = 0

# COMMAND ----------

# DBTITLE 1,Merge into silver table
logger.info(f"Merging {bronze_explode_df.count()} fixtures into {target_table} on key id")

# Create the silver table (empty, with the enforced schema) only on the very first run
if not spark.catalog.tableExists(target_table):
    bronze_explode_df.limit(0).write.saveAsTable(target_table)
    logger.info(f"Target table {target_table} did not exist — created with the enforced schema")

silver = DeltaTable.forName(spark, target_table)

# One-time migration: earlier silver versions may lack newer output columns (e.g. number_round).
# Silver is fully derived from bronze, so rebuild it from the current source (pure PySpark, no SQL DDL).
missing_columns = [c for c in bronze_explode_df.columns if c not in silver.toDF().columns]
if missing_columns:
    (
        bronze_explode_df
            .write.mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
    )
    silver = DeltaTable.forName(spark, target_table)
    logger.info(f"Schema migration: rebuilt {target_table} to add columns {missing_columns}")

# Idempotent MERGE keyed on fixture id:
#   * update an existing row whenever the derived values actually differ — this covers both
#     newer ingests (a newer ingest changes _ingested_at) AND transform-logic changes
#     (a code change alone does not bump _ingested_at, so a timestamp-only condition would skip it)
#   * NULL-safe comparison via IS DISTINCT FROM
#   * insert fixtures not yet in silver
row_differs = ' OR '.join(
    f't.{c} IS DISTINCT FROM s.{c}' for c in bronze_explode_df.columns if c != 'id'
)
(
    silver.alias('t')
        .merge(bronze_explode_df.alias('s'), 't.id = s.id')
        .whenMatchedUpdateAll(condition=row_differs)
        .whenNotMatchedInsertAll()
        .execute()
)

# Rows actually written, from the latest Delta commit's operation metrics
# (Delta API history + DataFrame column access — no spark.sql / selectExpr)
metrics = (
    DeltaTable.forName(spark, target_table)
        .history(1)
        .select(
            F.col("operationMetrics").getItem("numTargetRowsInserted").alias("inserted"),
            F.col("operationMetrics").getItem("numTargetRowsUpdated").alias("updated"),
        )
        .first()
)

logger.info(
    f"Silver transform run completed | {target_table}: "
    f"{metrics['inserted']} new fixtures inserted, {metrics['updated']} updated (newer ingest or changed values)"
)

# COMMAND ----------

# DBTITLE 1,Check silver sample rows
# Quick check: sample silver rows for one team (pure PySpark, no SQL)
display(
    spark.table(target_table)
        .filter(F.col("number_round") == 0)
        .orderBy(F.col("date").asc())
)

# COMMAND ----------

