# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest results JSON

# COMMAND ----------

display(dbutils.fs.mounts())
dbutils.fs.ls('/mnt/dlcoursestorage/raw')



# COMMAND ----------

# MAGIC %md
# MAGIC ###Read the file

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType, FloatType
from pyspark.sql.functions import current_timestamp

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

results_df = spark.read.json('/mnt/dlcoursestorage/raw/results.json',schema=results_schema)
results_df.display()

# COMMAND ----------

results_df.printSchema()
results_df.describe().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform the data

# COMMAND ----------

results_df_drop_cols = results_df.drop('statusId')

# COMMAND ----------

results_final_df = results_df_drop_cols.withColumnRenamed('constructorId','constructor_id') \
                                          .withColumnRenamed('driverId','driver_id') \
                                          .withColumnRenamed('fastestLap','fastest_lap') \
                                          .withColumnRenamed('fastestLapTime','fastest_lap_time') \
                                          .withColumnRenamed('fastestLapSpeed','fastest_lap_speed') \
                                          .withColumnRenamed('positionOrder','position_order') \
                                          .withColumnRenamed('positionText','position_text') \
                                          .withColumnRenamed('raceId','race_id') \
                                          .withColumnRenamed('resultId','result_id') \
                                          .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write back the data

# COMMAND ----------

results_final_df.write.mode('overwrite').partitionBy('race_id').parquet('/mnt/dlcoursestorage/processed/results')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dlcoursestorage/processed/results/race_id=951'))
spark.read.parquet('/mnt/dlcoursestorage/processed/results').display()
