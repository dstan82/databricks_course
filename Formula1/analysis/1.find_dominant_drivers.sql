-- Databricks notebook source
SELECT driver_name,
count(*) as total_races,
  SUM(normalized_point) as total_points,
  ROUND(AVG(normalized_point),2) as avg_point
FROM f1_presentation.calculated_race_results
WHERE race_year BETWEEN 2011 AND 2020
GROUP BY driver_name
HAVING COUNT(*) > 50
ORDER BY avg_point DESC
