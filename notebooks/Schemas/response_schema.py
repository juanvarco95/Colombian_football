# Databricks notebook source
# DBTITLE 1,Schema documentation
# MAGIC %md
# MAGIC # response_schema
# MAGIC
# MAGIC DDL string of the raw `response` JSON array stored in `football_col_dev.bronze.api_fixtures`, inferred from the actual bronze data. Used with `F.from_json` in **Transform Row Data**.
# MAGIC
# MAGIC Loaded via `%run ../Schemas/response_schema`.
# MAGIC

# COMMAND ----------

# DBTITLE 1,DDL schema of the raw response JSON array
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


# COMMAND ----------

