# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest laptimes folder/csv

# COMMAND ----------

dbutils.fs.mounts()
dbutils.fs.ls('/mnt/dlcoursestorage/raw')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_schema = StructType([StructField('raceId',IntegerType(),True), \
                             StructField('driverId',IntegerType(),False), \
                             StructField('lap',IntegerType(),True), \
                             StructField('position',IntegerType(),True), \
                             StructField('time',StringType(),True), \
                             StructField('milliseconds',IntegerType(),True)
])



# COMMAND ----------

lap_times_df = spark.read.csv('/mnt/dlcoursestorage/raw/lap_times/lap_times_split*.csv',schema=lap_times_schema)

# COMMAND ----------

lap_times_df.display()
lap_times_df.printSchema()
lap_times_df.describe().display()

# COMMAND ----------

final_lap_times_df = lap_times_df.withColumnRenamed('driverId','driver_id') \
                      .withColumnRenamed('raceId','race_id') \
                        .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

lap_times_df.display()

# COMMAND ----------

final_lap_times_df.write.mode('overwrite').parquet('/mnt/dlcoursestorage/processed/lap_times')

# COMMAND ----------

dbutils.fs.ls('/mnt/dlcoursestorage/processed/lap_times')
spark.read.parquet('/mnt/dlcoursestorage/processed/lap_times').display()
