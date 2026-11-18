"""Silver — clean fixtures, one row per fixture (latest ingest wins).

Flattens the raw JSON API response stored in bronze into a tabular dataset:
explodes the `response` array via the DDL schemas, derives round/number_round
from the API round label (unnumbered finals -> 0), derives is_finished from
the match status, deduplicates to the latest ingest per fixture id, and
enforces the explicit output schema. Data quality expectations enforce the
Fase 4 invariants: fixture id present, dates parseable, finished matches
carry goals, league stays in scope.
"""

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

from common.config import BRONZE_TABLE, LEAGUE_ID, SILVER_TABLE
from common.schemas import bronze_explode_schema, parameters_schema, response_schema


@dp.materialized_view(
    name=SILVER_TABLE,
    comment=(
        "One row per fixture: flattened from bronze, deduplicated to the "
        "latest ingest per fixture id, enforced output schema."
    ),
)
@dp.expect_or_fail("fixture id present", "id IS NOT NULL")
@dp.expect_or_fail("date parseable (dd-MM-yyyy)", "date IS NOT NULL")
@dp.expect_or_fail(
    "finished matches have goals",
    "(NOT is_finished) OR (home_goals IS NOT NULL AND away_goals IS NOT NULL)",
)
@dp.expect("league in scope", f"league_id = {LEAGUE_ID}")
def api_fixtures():
    bronze_df = spark.read.table(BRONZE_TABLE)

    # Bronze is append-only: the same fixture can arrive in several landed files.
    # Rank rows per fixture id by ingestion time and keep only the latest ingest.
    latest = Window.partitionBy("id").orderBy(F.desc("_ingested_at"))

    return (
        bronze_df
        .select(
            # ingestion metadata — ranks duplicates for dedup, and is kept in the output
            F.col("_ingested_at"),
            F.col("_source_file"),
            # explode the JSON response array into one row per fixture, typed by the DDL schemas
            F.explode(F.from_json(F.col("response"), response_schema)).alias("response"),
            F.from_json(F.col("parameters"), parameters_schema).alias("parameters"),
        )
        .withColumns({
            "id": F.col("response.fixture.id"),
            "season": F.col("parameters.season").cast("int"),
            # try_cast: null-safe date parsing (a malformed API value yields NULL instead of failing the run)
            "date": F.date_format(F.expr("try_cast(response.fixture.date AS DATE)"), "dd-MM-yyyy"),
            "league": F.col("response.league.name"),
            "round": F.col("response.league.round"),
            "referee": F.col("response.fixture.referee"),
            "city": F.col("response.fixture.venue.city"),
            "stadium": F.col("response.fixture.venue.name"),
            "home_team": F.col("response.teams.home.name"),
            "away_team": F.col("response.teams.away.name"),
            "home_logo": F.col("response.teams.home.logo"),
            "away_logo": F.col("response.teams.away.logo"),
            "home_goals": F.col("response.goals.home"),
            "away_goals": F.col("response.goals.away"),
            "home_halftime_goals": F.col("response.score.halftime.home"),
            "away_halftime_goals": F.col("response.score.halftime.away"),
            "home_penalty_goals": F.col("response.score.penalty.home"),
            "away_penalty_goals": F.col("response.score.penalty.away"),
            "home_winner": F.col("response.teams.home.winner"),
            "away_winner": F.col("response.teams.away.winner"),
            "league_id": F.col("response.league.id"),
            "status": F.col("response.fixture.status.short"),
            "status_long": F.col("response.fixture.status.long"),
            "venue_id": F.col("response.fixture.venue.id"),
            "home_team_id": F.col("response.teams.home.id"),
            "away_team_id": F.col("response.teams.away.id"),
        })
        .withColumn("is_finished", F.col("status").isin("FT", "AET", "PEN", "AWD", "WO"))
        # keep only the latest ingest per fixture id
        .withColumn("rn", F.row_number().over(latest))
        # split the API round label into phase and number, e.g. 'Apertura - 1' -> ('Apertura', 1)
        # element_at(-1) takes the LAST part, so 'Apertura - Quadrangular - 1' still yields 1
        .withColumn("round_parts", F.split(F.col("round"), "-"))
        .withColumn("round", F.trim(F.col("round_parts").getItem(0)))
        # try_cast: unnumbered rounds (e.g. 'Clausura - Final') become NULL instead of failing
        .withColumn("number_round", F.expr("try_cast(trim(element_at(round_parts, -1)) AS BIGINT)"))
        .withColumn("round", F.when(
            F.col("number_round").isNull(),
            F.concat(F.col("round"), F.lit(" "), F.lit("Final")),
        ).otherwise(F.col("round")))
        .withColumn("number_round", F.when(
            F.col("number_round").isNull(),
            F.lit(0),
        ).otherwise(F.col("number_round")))
        .filter(F.col("rn") == 1)
        .drop("response", "parameters", "rn", "round_parts")
        # enforce the explicit output schema (fixture columns incl. number_round + _ingested_at)
        .to(bronze_explode_schema)
    )