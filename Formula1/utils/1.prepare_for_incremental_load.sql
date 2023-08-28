-- Databricks notebook source
DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION '/mnt/dlcoursestorage/processed';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.mounts())

-- COMMAND ----------

SHOW DATABASES;
DESCRIBE DATABASE EXTENDED f1_raw;
SHOW TABLES in f1_processed;
DESCRIBE EXTENDED f1_processed.circuits

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION '/mnt/dlcoursestorage/presentation'
