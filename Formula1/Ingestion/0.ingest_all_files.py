# Databricks notebook source
v_result = dbutils.notebook.run('1.Ingest_circuits_file',0,{'p_data_source':'Ergast API','p_file_date':'2021-04-18'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('2.Ingest_races_file',0,{'p_data_source':'Ergast API','p_file_date':'2021-04-18'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('3.ingest_constructors_file',0,{'p_data_source':'Ergast API','p_file_date':'2021-04-18'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('4.ingest_drivers_file',0,{'p_data_source':'Ergast API','p_file_date':'2021-04-18'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('5.ingest_results_file',0,{'p_data_source':'Ergast API','p_file_date':'2021-04-18'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('6.ingest_pitstops_file',0,{'p_data_source':'Ergast API','p_file_date':'2021-04-18'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('7.ingest_laptimes_file',0,{'p_data_source':'Ergast API','p_file_date':'2021-04-18'})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run('8.ingest_qualifying_file',0,{'p_data_source':'Ergast API','p_file_date':'2021-04-18'})
