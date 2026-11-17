# Databricks notebook source
# DBTITLE 1,Schemas documentation
# MAGIC %md
# MAGIC # Schemas (Silver)
# MAGIC
# MAGIC Single source of truth for the schemas used by **Transform Row Data**. Loaded there via `%run ./Schemas`.
# MAGIC
# MAGIC * `bronze_explode_schema` — explicit `StructType` of the flattened silver output (one row per fixture), derived from the real structure of `bronze.api_fixtures`.
# MAGIC * `response_schema` — DDL of the raw `response` JSON array in `bronze.api_fixtures`.
# MAGIC * `parameters_schema` — DDL of the raw `parameters` JSON column.
# MAGIC

# COMMAND ----------

# DBTITLE 1,bronze_explode_schema (explicit output schema)
# Explicit schema for the flattened fixtures data, derived from the actual
# structure of the bronze data (bronze.api_fixtures -> explode(response)).
bronze_explode_schema = StructType([
    StructField("id",                  LongType(),    nullable=True),
    StructField("season",              StringType(),  nullable=True),
    StructField("date",                StringType(),  nullable=True),   # dd-MM-yyyy
    StructField("league",              StringType(),  nullable=True),
    StructField("round",              StringType(),  nullable=True),
    StructField("round_number",         LongType(),  nullable=True),
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
])


# COMMAND ----------

# DBTITLE 1,response_schema and parameters_schema (JSON DDL)
# DDL schemas inferred from the actual bronze data (bronze.api_fixtures)
response_schema = (
    "ARRAY<STRUCT<"
    "fixture: STRUCT<date: STRING, id: BIGINT, periods: STRUCT<first: BIGINT, second: BIGINT>, "
    "referee: STRING, status: STRUCT<elapsed: BIGINT, extra: BIGINT, long: STRING, short: STRING>, "
    "timestamp: BIGINT, timezone: STRING, venue: STRUCT<city: STRING, id: BIGINT, name: STRING>>, "
    "goals: STRUCT<away: BIGINT, home: BIGINT>, "
    "league: STRUCT<country: STRING, flag: STRING, id: BIGINT, logo: STRING, name: STRING, "
    "round: STRING, season: BIGINT, standings: BOOLEAN>, "
    "score: STRUCT<extratime: STRUCT<away: BIGINT, home: BIGINT>, "
    "fulltime: STRUCT<away: BIGINT, home: BIGINT>, "
    "halftime: STRUCT<away: BIGINT, home: BIGINT>, "
    "penalty: STRUCT<away: BIGINT, home: BIGINT>>, "
    "teams: STRUCT<away: STRUCT<id: BIGINT, logo: STRING, name: STRING, winner: BOOLEAN>, "
    "home: STRUCT<id: BIGINT, logo: STRING, name: STRING, winner: BOOLEAN>>>>"
)

parameters_schema = "STRUCT<league: STRING, season: STRING>"
