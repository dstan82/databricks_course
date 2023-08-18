# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

race_results.display()

# COMMAND ----------

race_results_2020 = race_results.filter(race_results.race_year == 2020)

# COMMAND ----------

race_results_2020.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

race_results_2020.select(count("*")).display()

# COMMAND ----------

race_results_2020.select(countDistinct("race_name")).display()

# COMMAND ----------

race_results_2020.select(sum("points")).display()

# COMMAND ----------

race_results_2020.filter(race_results_2020.driver_name == 'Lewis Hamilton').select(sum("points").alias('Points'),countDistinct('race_name').alias('number_of_races')).show()

# COMMAND ----------

race_results_2020.groupBy('driver_name').agg(sum('points').alias('points'),countDistinct('race_name').alias('number of races')).display()

# COMMAND ----------

race_results_2019_2020 = race_results.filter(race_results.race_year.isin(2019,2020))

race_results_grouped_2019_2020 = race_results_2019_2020.groupBy('race_year','driver_name').agg(sum('points').alias('points'),countDistinct('race_name').alias('number of races'))

race_results_grouped_2019_2020.display()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

defined_window = Window.partitionBy('race_year').orderBy(desc('points'))
race_results_grouped_2019_2020.withColumn('rank',rank().over(defined_window)).display()

# COMMAND ----------

#Window function
race_results_grouped_2019_2020.withColumn('rank',rank().over(Window.partitionBy('race_year').orderBy(desc('points')))).display()
