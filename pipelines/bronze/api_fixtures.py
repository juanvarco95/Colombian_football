"""Bronze — raw API-Football fixtures ingestion (Auto Loader).

Streaming table fed by Auto Loader from the landing volume: the raw JSON
files (one per API request) landed by the Landing notebook, kept as-is plus
ingestion metadata. The pipeline manages the Auto Loader checkpoint and
schema location automatically.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from common.config import BRONZE_TABLE, FIXTURES_DIR


@dp.table(
    name=BRONZE_TABLE,
    comment=(
        "Raw API-Football fixtures JSON, one row per landed file, plus "
        "_source_file/_file_modified_at/_ingested_at metadata."
    ),
)
def api_fixtures():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(FIXTURES_DIR)
        .withColumns(
            {
                "_source_file": F.col("_metadata.file_path"),
                "_file_modified_at": F.col("_metadata.file_modification_time"),
                "_ingested_at": F.current_timestamp(),
            }
        )
    )