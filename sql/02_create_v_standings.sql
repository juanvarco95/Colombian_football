-- Standings per season and tournament, regular phase only (the all-play-all fechas).
-- Ranked by points, goal difference, goals for — mirroring the official Dimayor tie-breakers.
-- The playoff (cuadrangulares) and final phases are excluded: they do not count towards the table.
-- Executed by notebooks/gold/01_Gold_Model; can also be run directly in the SQL editor.
CREATE OR REPLACE VIEW football_col_dev.gold.v_standings AS
SELECT
    f.season,
    f.tournament,
    t.team_name,
    COUNT(*) AS played,
    SUM(CASE WHEN f.points = 3 THEN 1 ELSE 0 END) AS won,
    SUM(CASE WHEN f.points = 1 THEN 1 ELSE 0 END) AS drawn,
    SUM(CASE WHEN f.points = 0 THEN 1 ELSE 0 END) AS lost,
    SUM(f.goals_for) AS goals_for,
    SUM(f.goals_against) AS goals_against,
    SUM(f.goals_for - f.goals_against) AS goal_diff,
    SUM(f.points) AS points,
    RANK() OVER (
        PARTITION BY f.season, f.tournament
        ORDER BY SUM(f.points) DESC, SUM(f.goals_for - f.goals_against) DESC, SUM(f.goals_for) DESC
    ) AS position
FROM football_col_dev.gold.fact_team_match f
JOIN football_col_dev.gold.dim_team t
  ON f.team_id = t.team_id AND t.is_current
WHERE f.phase = 'regular'
GROUP BY f.season, f.tournament, t.team_name