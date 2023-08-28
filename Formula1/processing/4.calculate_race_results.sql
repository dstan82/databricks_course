-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

SELECT current_database();
SHOW TABLES;

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT rac.race_year,
      cons.name as team_name,
      dr.name driver_name,
      res.position,
      res.points,
      11 - res.position as normalized_point
FROM f1_processed.results res JOIN drivers dr ON res.driver_id = dr.driver_id
JOIN f1_processed.constructors cons ON cons.constructor_id = res.constructor_id
JOIN f1_processed.races rac ON rac.race_id = res.race_id
WHERE res.position <= 10

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------


