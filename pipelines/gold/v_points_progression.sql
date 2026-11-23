-- Cumulative points per team per matchday (regular phase), one line per team in a dashboard chart.
-- dim_team is SCD2: join the current version of each team (__END_AT IS NULL).
-- :catalog is a pipeline parameter (the bundle sets it per target — dev/prod).
CREATE VIEW IDENTIFIER(:catalog || '.gold.v_points_progression')
COMMENT 'Cumulative points per team per matchday (regular phase)'
AS
SELECT
    f.season,
    f.tournament,
    t.team_name,
    f.match_date,
    f.number_round,
    SUM(f.points) OVER (
        PARTITION BY f.season, f.tournament, f.team_id
        ORDER BY f.match_date, f.number_round
    ) AS cumulative_points
FROM IDENTIFIER(:catalog || '.gold.fact_team_match') f
JOIN IDENTIFIER(:catalog || '.gold.dim_team') t
  ON f.team_id = t.team_id AND t.__END_AT IS NULL
WHERE f.phase = 'regular'