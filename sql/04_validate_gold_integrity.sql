-- Integrity checks for the gold model: grain, arithmetic, and standings shape.
-- Every check must return 0 violations.
-- Executed by notebooks/gold/01_Gold_Model; can also be run directly in the SQL editor.
SELECT 'fact grain: duplicated match_id+team_id (must be 0)' AS check, COUNT(*) AS violations
FROM (
  SELECT match_id, team_id FROM football_col_dev.gold.fact_team_match
  GROUP BY match_id, team_id HAVING COUNT(*) > 1
)
UNION ALL
SELECT 'matches without exactly 2 fact rows (must be 0)', COUNT(*)
FROM (
  SELECT match_id FROM football_col_dev.gold.fact_team_match
  GROUP BY match_id HAVING COUNT(*) <> 2
)
UNION ALL
SELECT 'standings: season-tournaments without exactly 20 teams (must be 0)', COUNT(*)
FROM (
  SELECT season, tournament FROM football_col_dev.gold.v_standings
  GROUP BY season, tournament HAVING COUNT(*) <> 20
)
UNION ALL
SELECT 'standings: points formula broken (must be 0)', COUNT(*)
FROM (
  SELECT season, tournament, SUM(points) AS p, 3*SUM(won)+SUM(drawn) AS expected
  FROM football_col_dev.gold.v_standings
  GROUP BY season, tournament HAVING p <> expected
)