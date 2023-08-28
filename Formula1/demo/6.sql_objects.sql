-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database
-- MAGIC

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;


-- COMMAND ----------

USE demo;


-- COMMAND ----------

SELECT current_database();


-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').mode('overwrite').saveAsTable('demo.race_results_python') #create managed table in python

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_python

-- COMMAND ----------

SELECT * FROM demo.race_results_python

WHERE race_year = 2020

-- COMMAND ----------

--Create managed table in SQL
CREATE TABLE race_results_sql
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED race_results_sql;

-- COMMAND ----------

SELECT * FROM demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').mode('overwrite').option('path',f'{demo_folder_path}/race_results_ext_py').saveAsTable('demo.race_results_ext_py') #create external table using python

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %python
-- MAGIC demo_folder_path

-- COMMAND ----------

-- Create external empty table, using parquet format, in a specified location where the actual data will be located. For now it's just the schema/metadata stored in databricks, nothing yet in ADLS

CREATE TABLE demo.race_results_ext_sql 
(race_year int,
race_name string,
race_date timestamp,
circuit_location string,
driver_name string,
driver_number int,
driver_nationality string,
team string,
grid int,
fastest_lap int,
race_time string,
points float,
position int,
created_date timestamp
)
USING PARQUET
LOCATION '/mnt/dlcoursestorage/demo/race_results_ext_sql'

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

--Insert data into the empty table from another table
INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES in demo

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql --as this is an external data the DROP will just drop the schema. Data remains in ADLS and can be accessed.

-- COMMAND ----------

SHOW TABLES in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Views on tables
-- MAGIC ###Learning Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent View

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results AS
SELECT * FROM race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM v_race_results --Only valid in the current session

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results 
AS
SELECT * FROM race_results_python
WHERE race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results --global views always need the global_temp. prefix

-- COMMAND ----------

SHOW TABLES in global_temp;

-- COMMAND ----------

--- creating permanent view ---
CREATE OR REPLACE VIEW demo.pv_race_results 
AS
SELECT * FROM demo.race_results_python
WHERE race_year = 2000;

-- COMMAND ----------

SHOW TABLES in demo

-- COMMAND ----------

SELECT * FROM demo.pv_race_results
