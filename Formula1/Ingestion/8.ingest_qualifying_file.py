# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest qualifying JSON

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.fs.mounts()
display(dbutils.fs.ls(f'{raw_folder_path}/qualifying'))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType

# COMMAND ----------

qualifying_schema = StructType([StructField('qualifyId',IntegerType(),False), \
                             StructField('raceId',IntegerType(),True), \
                             StructField('driverId',IntegerType(),True), \
                             StructField('constructorId',IntegerType(),True), \
                             StructField('number',IntegerType(),True), \
                             StructField('position',IntegerType(),True), \
                             StructField('q1',StringType(),True), \
                             StructField('q2',StringType(),True), \
                             StructField('q3',StringType(),True), \
])

# COMMAND ----------

qualifying_df = spark.read.json(f'{raw_folder_path}/qualifying/qualifying_split_*.json',multiLine=True,schema=qualifying_schema)

# COMMAND ----------

qualifying_df.display()
qualifying_df.printSchema()
qualifying_df.describe().display()

# COMMAND ----------

col_rename_qualifying_df = qualifying_df.withColumnRenamed('qualifyId','qualify_id') \
                      .withColumnRenamed('raceId','race_id') \
                      .withColumnRenamed('driverId','driver_id') \
                      .withColumnRenamed('constructorId','constructor_id') \
                          
final_qualifying_df = add_ingestion_date(col_rename_qualifying_df)

# COMMAND ----------

final_qualifying_df.display()

# COMMAND ----------

final_qualifying_df.write.mode('overwrite').parquet(f'{processed_folder_path}/qualifying')

# COMMAND ----------

dbutils.fs.ls('/mnt/dlcoursestorage/processed/qualifying')
spark.read.parquet('/mnt/dlcoursestorage/processed/qualifying').display()
