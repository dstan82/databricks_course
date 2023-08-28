-- Databricks notebook source
SELECT driver_name,
count(*) as total_races,
  SUM(normalized_point) as total_points,
  ROUND(AVG(normalized_point),2) as avg_point,
  RANK() OVER (ORDER BY AVG(normalized_point) DESC) driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(*) > 50
ORDER BY avg_point DESC

-- COMMAND ----------

WITH top_drivers AS (
SELECT driver_name,
count(*) as total_races,
  SUM(normalized_point) as total_points,
  ROUND(AVG(normalized_point),2) as avg_point,
  RANK() OVER (ORDER BY AVG(normalized_point) DESC) driver_rank
FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING COUNT(*) > 50
ORDER BY avg_point DESC
LIMIT 5  
)

SELECT race_year,
crr.driver_name,
count(*) as total_races,
  SUM(normalized_point) as total_points,
  ROUND(AVG(normalized_point),2) as avg_point
FROM f1_presentation.calculated_race_results crr JOIN top_drivers td ON crr.driver_name = td.driver_name
GROUP BY crr.driver_name,race_year
ORDER BY race_year,avg_point DESC
