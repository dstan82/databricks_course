# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest pitstops JSON

# COMMAND ----------

dbutils.fs.mounts()
dbutils.fs.ls('/mnt/dlcoursestorage/raw')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_schema = StructType([StructField('driverId',IntegerType(),False), \
                             StructField('duration',StringType(),True), \
                             StructField('lap',IntegerType(),True), \
                             StructField('milliseconds',IntegerType(),True), \
                             StructField('raceId',IntegerType(),True), \
                             StructField('stop',IntegerType(),True), \
                             StructField('time',StringType(),True)
])



# COMMAND ----------

pitstops_df = spark.read.json('/mnt/dlcoursestorage/raw/pit_stops.json',multiLine=True,schema=pit_stops_schema)

# COMMAND ----------

pitstops_df.display()
pitstops_df.printSchema()
pitstops_df.describe().display()

# COMMAND ----------

final_df = pitstops_df.withColumnRenamed('driverId','driver_id') \
                      .withColumnRenamed('raceId','race_id') \
                        .withColumn('ingestion_date',current_timestamp())



# COMMAND ----------

final_df.display()

# COMMAND ----------

final_df.write.mode('overwrite').parquet('/mnt/dlcoursestorage/processed/pitstops')

# COMMAND ----------

dbutils.fs.ls('/mnt/dlcoursestorage/processed/pitstops')
spark.read.parquet('/mnt/dlcoursestorage/processed/pitstops').display()

# COMMAND ----------


