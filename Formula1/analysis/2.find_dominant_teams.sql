-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------

SELECT team_name,
count(*) as total_races,
  SUM(normalized_point) as total_points,
  ROUND(AVG(normalized_point),2) as avg_point
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING COUNT(*) > 100
ORDER BY avg_point DESC

-- COMMAND ----------


