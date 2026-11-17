# Databricks notebook source
# DBTITLE 1,Schema documentation
# MAGIC %md
# MAGIC # parameters_schema
# MAGIC
# MAGIC DDL string of the raw `parameters` JSON column stored in `football_col_dev.bronze.api_fixtures` (e.g. `{"league": "239", "season": "2022"}`). Used with `F.from_json` in **Transform Row Data**.
# MAGIC
# MAGIC Loaded via `%run ../Schemas/parameters_schema`.
# MAGIC

# COMMAND ----------

# DBTITLE 1,DDL schema of the raw parameters JSON column
parameters_schema = "STRUCT<league: STRING, season: STRING>"


# COMMAND ----------

