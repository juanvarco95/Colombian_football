-- Cumulative points per team per matchday (regular phase), one line per team in a dashboard chart.
-- Executed by notebooks/gold/01_Gold_Model; can also be run directly in the SQL editor.
CREATE OR REPLACE VIEW football_col_dev.gold.v_points_progression AS
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
FROM football_col_dev.gold.fact_team_match f
JOIN football_col_dev.gold.dim_team t
  ON f.team_id = t.team_id AND t.is_current
WHERE f.phase = 'regular'