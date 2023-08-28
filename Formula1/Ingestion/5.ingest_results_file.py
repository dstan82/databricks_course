# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest results JSON

# COMMAND ----------

dbutils.widgets.text('p_data_source','') # define widget
v_data_source = dbutils.widgets.get('p_data_source') # retrieve the parameter from the widget

print(v_data_source)

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-28') # define widget
v_file_date = dbutils.widgets.get('p_file_date') # retrieve the parameter from the widget

print(v_file_date)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

display(dbutils.fs.mounts())
dbutils.fs.ls(f'{raw_folder_path}')



# COMMAND ----------

# MAGIC %md
# MAGIC ###Read the file

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

results_schema = StructType([StructField('constructorId', IntegerType(), False), \
                             StructField('driverId', IntegerType(), False), \
                             StructField('fastestLap', IntegerType(), False), \
                             StructField('fastestLapSpeed', StringType(), False), \
                             StructField('fastestLapTime', StringType(), False), \
                             StructField('grid', IntegerType(), False), \
                             StructField('laps', IntegerType(), False), \
                             StructField('milliseconds', IntegerType(), False), \
                             StructField('number', IntegerType(), False), \
                             StructField('points', FloatType(), False), \
                             StructField('position', IntegerType(), False), \
                             StructField('positionOrder', IntegerType(), False), \
                             StructField('positionText', StringType(), False), \
                             StructField('raceId', IntegerType(), False), \
                             StructField('rank', IntegerType(), False), \
                             StructField('resultId', IntegerType(), False), \
                             StructField('statusId', IntegerType(), False), \
                             StructField('time', StringType(), False)
])

# COMMAND ----------

results_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/results.json',schema=results_schema)
results_df.display()
results_df.createOrReplaceTempView('tv_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE tv_results

# COMMAND ----------

results_df.printSchema()
results_df.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform the data

# COMMAND ----------

results_df_drop_cols = results_df.drop('statusId')

# COMMAND ----------

from pyspark.sql.functions import lit

results_col_rename_df = results_df_drop_cols.withColumnRenamed('constructorId','constructor_id') \
                                          .withColumnRenamed('driverId','driver_id') \
                                          .withColumnRenamed('fastestLap','fastest_lap') \
                                          .withColumnRenamed('fastestLapTime','fastest_lap_time') \
                                          .withColumnRenamed('fastestLapSpeed','fastest_lap_speed') \
                                          .withColumnRenamed('positionOrder','position_order') \
                                          .withColumnRenamed('positionText','position_text') \
                                          .withColumnRenamed('raceId','race_id') \
                                          .withColumnRenamed('resultId','result_id') \
                                          .withColumn('data_source', lit(v_data_source)) \
                                          .withColumn('file_date',lit(v_file_date))

results_final_df = add_ingestion_date(results_col_rename_df)                                          

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write back the data

# COMMAND ----------

# MAGIC %md
# MAGIC Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select('race_id').distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists('f1_processed.results')):
#         spark.sql(f'ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})')
# print(race_id_list)

# COMMAND ----------

# results_final_df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC Method 2

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results
# MAGIC SHOW TABLES IN f1_processed;
# MAGIC --SHOW DATABASES;
# MAGIC --SHOW TABLES IN 

# COMMAND ----------

# results_final_partition_df = move_partition_column(results_final_df, 'race_id')

# COMMAND ----------

#function in 'includes' notebook - expects: Schema, Table, DataFrame, Partitioning_Column
incremental_load('f1_processed','results',results_final_df,'race_id')

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls('/mnt/dlcoursestorage/processed/results/'))
spark.read.parquet('/mnt/dlcoursestorage/processed/results').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*) FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit('Success')
