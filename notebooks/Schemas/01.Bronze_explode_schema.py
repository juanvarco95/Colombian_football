# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Schema documentation
# MAGIC %md
# MAGIC # bronze_explode_schema
# MAGIC
# MAGIC Explicit `StructType` of the flattened silver output — one row per fixture — derived from the actual structure of `bronze.api_fixtures` (`explode(response)`).
# MAGIC
# MAGIC Besides the fixture columns (including the derived `number_round` — the numeric part of the API round label) it carries `_ingested_at` (ingestion timestamp from bronze), so the silver MERGE can update an existing fixture when a newer ingest arrives.
# MAGIC
# MAGIC Loaded by **Transform Row Data** via `%run ../Schemas/bronze_explode_schema` (after `%run ../common/01.Setup`, which provides the `pyspark.sql.types` imports; `TimestampType` is imported locally).
# MAGIC

# COMMAND ----------

# DBTITLE 1,Explicit output schema of the flattened fixtures data
from pyspark.sql.types import TimestampType

bronze_explode_schema = StructType([
    StructField("id",                  LongType(),    nullable=True),
    StructField("season",              StringType(),  nullable=True),
    StructField("date",                StringType(),  nullable=True),   # dd-MM-yyyy
    StructField("league",              StringType(),  nullable=True),
    StructField("round",              StringType(),  nullable=True),
    StructField("number_round",        LongType(),    nullable=True),  # numeric part of the round label; NULL for unnumbered rounds (e.g. Final)
    StructField("referee",              StringType(),  nullable=True),
    StructField("city",                StringType(),  nullable=True),
    StructField("stadium",              StringType(),  nullable=True),
    StructField("home_team",            StringType(),  nullable=True),
    StructField("away_team",            StringType(),  nullable=True),
    StructField("home_logo",            StringType(),  nullable=True),
    StructField("away_logo",            StringType(),  nullable=True),
    StructField("home_goals",           LongType(),    nullable=True),
    StructField("away_goals",           LongType(),    nullable=True),
    StructField("home_halftime_goals",  LongType(),    nullable=True),
    StructField("away_halftime_goals",  LongType(),    nullable=True),
    StructField("home_penalty_goals",   LongType(),    nullable=True),
    StructField("away_penalty_goals",   LongType(),    nullable=True),
    StructField("home_winner",          BooleanType(), nullable=True),
    StructField("away_winner",          BooleanType(), nullable=True),
    StructField("league_id",    LongType(),    nullable=True),
    StructField("status",       StringType(),  nullable=True),
    StructField("status_long",  StringType(),  nullable=True),
    StructField("is_finished",  BooleanType(), nullable=True),
    StructField("venue_id",     LongType(),    nullable=True),
    StructField("home_team_id", LongType(),    nullable=True),
    StructField("away_team_id", LongType(),    nullable=True),
    # ingestion timestamp from bronze — lets the silver MERGE update a fixture on a newer ingest
    StructField("_ingested_at",          TimestampType(), nullable=True),
    StructField("_source_file",            StringType(),  nullable=True)
])
