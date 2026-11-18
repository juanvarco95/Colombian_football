"""JSON parsing schemas — mirror of the `notebooks/Schemas/` DDL notebooks.

`response_schema` and `parameters_schema` type the raw JSON string columns of
bronze with `F.from_json`; `bronze_explode_schema` is the enforced output
schema of the flattened silver dataset (one row per fixture).
"""

from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType, TimestampType

# DDL of the raw `response` JSON array (inferred from the actual bronze data)
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

# DDL of the raw `parameters` JSON column, e.g. {"league": "239", "season": "2022"}
parameters_schema = "STRUCT<league: STRING, season: STRING>"

# Enforced output schema of the flattened fixtures dataset (one row per fixture)
bronze_explode_schema = StructType([
    StructField("id", LongType(), nullable=True),
    StructField("season", StringType(), nullable=True),
    StructField("date", StringType(), nullable=True),  # dd-MM-yyyy
    StructField("league", StringType(), nullable=True),
    StructField("round", StringType(), nullable=True),
    StructField("number_round", LongType(), nullable=True),  # numeric part of the round label; 0 = unnumbered (e.g. Final)
    StructField("referee", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("stadium", StringType(), nullable=True),
    StructField("home_team", StringType(), nullable=True),
    StructField("away_team", StringType(), nullable=True),
    StructField("home_logo", StringType(), nullable=True),
    StructField("away_logo", StringType(), nullable=True),
    StructField("home_goals", LongType(), nullable=True),
    StructField("away_goals", LongType(), nullable=True),
    StructField("home_halftime_goals", LongType(), nullable=True),
    StructField("away_halftime_goals", LongType(), nullable=True),
    StructField("home_penalty_goals", LongType(), nullable=True),
    StructField("away_penalty_goals", LongType(), nullable=True),
    StructField("home_winner", BooleanType(), nullable=True),
    StructField("away_winner", BooleanType(), nullable=True),
    StructField("league_id", LongType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("status_long", StringType(), nullable=True),
    StructField("is_finished", BooleanType(), nullable=True),
    StructField("venue_id", LongType(), nullable=True),
    StructField("home_team_id", LongType(), nullable=True),
    StructField("away_team_id", LongType(), nullable=True),
    # ingestion timestamp from bronze — ranks duplicates for the dedup window
    StructField("_ingested_at", TimestampType(), nullable=True),
    StructField("_source_file", StringType(), nullable=True),
])