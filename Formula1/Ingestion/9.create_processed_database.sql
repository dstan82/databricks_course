-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION '/mnt/dlcoursestorage/processed'

-- COMMAND ----------

DESCRIBE DATABASE f1_processed;
