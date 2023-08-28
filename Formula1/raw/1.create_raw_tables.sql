-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits 
(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS (path '/mnt/dlcoursestorage/raw/circuits.csv', header true);


-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races 
(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path '/mnt/dlcoursestorage/raw/races.csv', header true);


-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Create tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create constructors table
-- MAGIC - Single Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors 
(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path '/mnt/dlcoursestorage/raw/constructors.json');

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create drivers table
-- MAGIC - Single Line JSON
-- MAGIC - Complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers 
(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path '/mnt/dlcoursestorage/raw/drivers.json')

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create results table
-- MAGIC - Simple line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results 
(
  constructorId int,
  driverId int,
  fastestLap int,
  fastestLapSpeed string,
  fastestLapTime string,
  grid int,
  laps int,
  milliseconds int,
  number int,
  points float,
  position int,
  positionOrder int,
  positionText string,
  raceId int,
  rank int,
  resultId int,
  statusId int,
  time string
)
USING JSON
OPTIONS (path '/mnt/dlcoursestorage/raw/results.json')

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create pit stops table
-- MAGIC - Multi Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops
(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds STRING,
  raceId INT,
  stop INT,
  time STRING
)
USING JSON
OPTIONS (path '/mnt/dlcoursestorage/raw/pit_stops.json', multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create tables for list of files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Lap Times Table
-- MAGIC - CSV file
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times
(
driverId INT,
lap INT,
position INT,
time STRING,
milliseconds INT
)
USING CSV
OPTIONS (path '/mnt/dlcoursestorage/raw/lap_times/lap_times_split*.csv')

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create Qualifying Table
-- MAGIC - JSON file
-- MAGIC - MultiLine JSON
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying
(
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING
)
USING JSON
OPTIONS (path '/mnt/dlcoursestorage/raw/qualifying/qualifying_split_*.json', multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying

-- COMMAND ----------

DESCRIBE EXTENDED f1_raw.qualifying

-- COMMAND ----------


