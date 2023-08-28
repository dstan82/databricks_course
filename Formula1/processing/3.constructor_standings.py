# Databricks notebook source
dbutils.widgets.text('p_file_date','2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import sum, count, col,when

# COMMAND ----------

race_results_all = spark.read.parquet(f'{presentation_folder_path}/race_results') #reads all the data from the same layer - this notebook is based on race_results
race_results_list = race_results_all.filter(race_results_all.file_date == v_file_date).select('race_year').distinct().collect() #collect - transforms the data into a list of objects | Selects all records with file_date = parameter data and creates a distinct race_years list in that selection. We will need whole 'year' for later calculation so regardless of the parameter date we will update 'whole' race_years

#transforms the list of 'objects' intor list of 'strings'
race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------


race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results').filter(col("race_year").isin(race_year_list)) #Reads from source 'race_results' the 'whole' years contained in the records matching file_date parameter

# COMMAND ----------

race_results_df.display()

# COMMAND ----------

#count with condition
constructor_standings_df = race_results_df.groupBy('race_year','team').agg(sum('points').alias('total_points'), count(when(col('position')==1,True)).alias('wins'))

# COMMAND ----------

constructor_standings_df.filter(constructor_standings_df.race_year == 2020).display()

# COMMAND ----------

from pyspark.sql.functions import desc, rank
from pyspark.sql.window import Window

constructor_standings_final_df = constructor_standings_df.withColumn('rank',rank().over(Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))))


# COMMAND ----------

constructor_standings_final_df.where('race_year = 2020').display()

# COMMAND ----------

#constructor_standings_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.driver_constructor')

incremental_load('f1_presentation','driver_constructor',constructor_standings_final_df,'race_year')

# COMMAND ----------

spark.read.parquet(f'{presentation_folder_path}/driver_constructor').filter('race_year = 2020').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, count(*)
# MAGIC FROM f1_presentation.driver_constructor
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year DESC

# COMMAND ----------

dbutils.notebook.exit('Success')
