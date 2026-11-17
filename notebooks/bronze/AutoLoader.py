# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Notebook documentation
# MAGIC %md
# MAGIC # AutoLoader (Bronze ingestion)
# MAGIC
# MAGIC Incrementally ingests the raw JSON files landed by the **Landing** notebook into the bronze Delta table.
# MAGIC
# MAGIC * **Source:** `/Volumes/football_col_dev/landing/raw/api_football/fixtures/` (JSON files)
# MAGIC * **Target:** `football_col_dev.bronze.api_fixtures` (streaming append + metadata columns)
# MAGIC
# MAGIC **How it works**
# MAGIC 1. Loads shared imports and the run logger from `../common/setup`.
# MAGIC 2. Auto Loader (`cloudFiles`) picks up new files; schema evolution is tracked in `_schemas/fixtures`.
# MAGIC 3. Appends rows plus `_source_file` and `_ingested_at` metadata to bronze, checkpointed at `_checkpoints/bronze_fixtures`.
# MAGIC 4. Trigger `availableNow=True` processes all pending files and then stops. Rows written are logged.
# MAGIC
# MAGIC **Run:** top to bottom, after the Landing notebook has landed new files.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Load shared setup (imports + logger)
# MAGIC %run ../common/Setup

# COMMAND ----------

# DBTITLE 1,Configure Auto Loader read stream
logger = get_logger("autoloader")

RAW_PATH = "/Volumes/football_col_dev/landing/raw"
target_table = "football_col_dev.bronze.api_fixtures"

logger.info(f"Bronze ingestion run started | source: {RAW_PATH}/api_football/fixtures/ | target: {target_table}")

df_raw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{RAW_PATH}/_schemas/fixtures")
        .load(f"{RAW_PATH}/api_football/fixtures/")
)



# COMMAND ----------

# DBTITLE 1,Stream to bronze table (availableNow trigger)
bronze_query = (
    df_raw.withColumns({
            "_source_file"  : F.col("_metadata.file_path"),
            "_ingested_at"  : F.current_timestamp()
        }
    )
    .writeStream
    .option("checkpointLocation", f"{RAW_PATH}/_checkpoints/bronze_fixtures")
    .trigger(availableNow=True)
    .toTable(target_table)
)

bronze_query.awaitTermination()

rows_written = (
    bronze_query.lastProgress["numInputRows"]
    if bronze_query.lastProgress else 0
)
logger.info(f"Bronze ingestion completed | {rows_written} new rows written to {target_table}")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from football_col_dev.bronze.api_fixtures

# COMMAND ----------

