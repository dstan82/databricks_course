# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest qualifying JSON

# COMMAND ----------

dbutils.fs.mounts()
display(dbutils.fs.ls('/mnt/dlcoursestorage/raw/qualifying'))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType
from pyspark.sql.functions import current_timestamp

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

qualifying_df = spark.read.json('/mnt/dlcoursestorage/raw/qualifying/qualifying_split_*.json',multiLine=True,schema=qualifying_schema)

# COMMAND ----------

qualifying_df.display()
qualifying_df.printSchema()
qualifying_df.describe().display()

# COMMAND ----------

final_qualifying_df = qualifying_df.withColumnRenamed('qualifyId','qualify_id') \
                      .withColumnRenamed('raceId','race_id') \
                      .withColumnRenamed('driverId','driver_id') \
                      .withColumnRenamed('constructorId','constructor_id') \
                      .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

final_qualifying_df.display()

# COMMAND ----------

final_qualifying_df.write.mode('overwrite').parquet('/mnt/dlcoursestorage/processed/qualifying')

# COMMAND ----------

dbutils.fs.ls('/mnt/dlcoursestorage/processed/qualifying')
spark.read.parquet('/mnt/dlcoursestorage/processed/qualifying').display()

# COMMAND ----------


