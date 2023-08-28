# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races').where("race_year = 2019").withColumnRenamed('name', 'races_name')
circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits').withColumnRenamed('name', 'circuits_name')

# COMMAND ----------

races_df.display()

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id,"inner") #inner join, keeps all columns, both key columns (doubled)

# COMMAND ----------

race_circuits_df.display()

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,'circuit_id',"inner") #inner join, keeps only 1 key colummn
race_circuits_df.display()

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,'circuit_id',"anti") #Anti join, keeps only not matching records
race_circuits_df.display()

# COMMAND ----------

race_circuits_select_df = race_circuits_df.select(race_circuits_df.circuits_name,race_circuits_df.location,race_circuits_df.country, race_circuits_df.races_name, race_circuits_df.round)

# COMMAND ----------

race_circuits_select_df.display()
