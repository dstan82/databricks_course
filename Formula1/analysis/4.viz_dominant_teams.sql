-- Databricks notebook source
SELECT team_name,
count(*) as total_races,
  SUM(normalized_point) as total_points,
  ROUND(AVG(normalized_point),2) as avg_point,
  RANK() OVER (ORDER BY AVG(normalized_point) DESC) team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(*) > 100
ORDER BY avg_point DESC

-- COMMAND ----------

WITH top_teams AS (
SELECT team_name,
count(*) as total_races,
  SUM(normalized_point) as total_points,
  ROUND(AVG(normalized_point),2) as avg_point,
  RANK() OVER (ORDER BY AVG(normalized_point) DESC) team_rank
FROM f1_presentation.calculated_race_results
GROUP BY team_name
HAVING COUNT(*) > 100
ORDER BY avg_point DESC
LIMIT 5 
)

SELECT race_year,
crr.team_name,
count(*) as total_races,
  SUM(normalized_point) as total_points,
  ROUND(AVG(normalized_point),2) as avg_point
FROM f1_presentation.calculated_race_results crr JOIN top_teams tt ON crr.team_name = tt.team_name
GROUP BY crr.team_name,race_year
ORDER BY race_year,avg_point DESC
