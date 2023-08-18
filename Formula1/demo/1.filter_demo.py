# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

races_filter_df = races_df.filter('race_year = 2021 and round <= 5')

races_filter_df = races_df.filter((races_df.race_year == 2021) & (races_df.round <= 5))

# COMMAND ----------

races_filter_df.display()
