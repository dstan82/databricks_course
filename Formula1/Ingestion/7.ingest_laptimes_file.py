# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest laptimes folder/csv

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

lap_times_df = spark.read.csv(f'{raw_folder_path}/lap_times/lap_times_split*.csv',schema=lap_times_schema)

# COMMAND ----------

lap_times_df.display()
lap_times_df.printSchema()
lap_times_df.describe().display()

# COMMAND ----------

col_rename_lap_times_df = lap_times_df.withColumnRenamed('driverId','driver_id') \
                      .withColumnRenamed('raceId','race_id') \

final_lap_times_df = add_ingestion_date(col_rename_lap_times_df)

# COMMAND ----------

final_lap_times_df.write.mode('overwrite').parquet(f'{processed_folder_path}/lap_times')

# COMMAND ----------

dbutils.fs.ls('/mnt/dlcoursestorage/processed/lap_times')
spark.read.parquet('/mnt/dlcoursestorage/processed/lap_times').display()
