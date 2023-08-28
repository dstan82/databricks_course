# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest laptimes folder/csv

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

dbutils.fs.mounts()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType([StructField('raceId',IntegerType(),True), \
                             StructField('driverId',IntegerType(),False), \
                             StructField('lap',IntegerType(),True), \
                             StructField('position',IntegerType(),True), \
                             StructField('time',StringType(),True), \
                             StructField('milliseconds',IntegerType(),True)
])

# COMMAND ----------

lap_times_df = spark.read.csv(f'{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv',schema=lap_times_schema)

# COMMAND ----------

lap_times_df.display()
lap_times_df.printSchema()
lap_times_df.describe().display()

# COMMAND ----------

from pyspark.sql.functions import lit

col_rename_lap_times_df = lap_times_df.withColumnRenamed('driverId','driver_id') \
                      .withColumnRenamed('raceId','race_id') \
                      .withColumn('data_source', lit(v_data_source))

final_lap_times_df = add_ingestion_date(col_rename_lap_times_df)

# COMMAND ----------

#final_lap_times_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.lap_times')

#function in 'includes' notebook - expects: Schema, Table, DataFrame, Partitioning_Column
incremental_load('f1_processed','lap_times',final_lap_times_df,'race_id')

# COMMAND ----------

dbutils.fs.ls('/mnt/dlcoursestorage/processed/lap_times')
spark.read.parquet('/mnt/dlcoursestorage/processed/lap_times').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*) FROM f1_processed.lap_times
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit('Success')
