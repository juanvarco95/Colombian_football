-- Regular-phase leaders per season and tournament (sanity check vs public records).
-- Executed by notebooks/gold/01_Gold_Model; can also be run directly in the SQL editor.
SELECT season, tournament, team_name, played, won, drawn, lost, goal_diff, points, position
FROM football_col_dev.gold.v_standings
WHERE position = 1
ORDER BY season, tournament