# Databricks notebook source
# MAGIC %md
# MAGIC ##Access dataframes using SQL
# MAGIC
# MAGIC ####Objectives
# MAGIC 1. Create temporary view on dataframes
# MAGIC 2. Access the view from SQL cell
# MAGIC 3. Access the biew from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

# TEMPORARY VIEWS - only available in the current session
race_results_df.createTempView('v_race_results') #creates temp view, if temp view exists it raises an error
race_results_df.createOrReplaceTempView('v_race_results') #creates temp view, if temp view exists it overwrites

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH driver_nationality as (
# MAGIC SELECT driver_nationality,driver_name
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020
# MAGIC GROUP BY driver_nationality, driver_name )
# MAGIC
# MAGIC SELECT driver_nationality, count(*)
# MAGIC FROM driver_nationality
# MAGIC GROUP BY driver_nationality

# COMMAND ----------


p_race_year = 2020
spark.sql(f'SELECT * FROM v_race_results WHERE race_year = {p_race_year}').display()

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView('gv_race_results') #creates global view

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM global_temp.gv_race_results -- need to specify "global_temp."
