# Databricks notebook source
# MAGIC %md
# MAGIC ###Step1 - Read the CSV file using thwe spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

#importing functions
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Checking mount path
# dbutils.fs.mounts()
raw_folder_path

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dlcoursestorage/raw

# COMMAND ----------

#setting schema
races_schema = StructType(fields=[StructField('raceId', IntegerType(),False),
                                 StructField('year', IntegerType(),True),
                                 StructField('round', IntegerType(),True),
                                 StructField('circuitId', IntegerType(),True),
                                 StructField('name', StringType(),True),
                                 StructField('date', DateType(),True),
                                 StructField('time', StringType(),True),
                                 StructField('url', StringType(),True)
])

# COMMAND ----------

raw_race_df = spark.read.csv(f'{raw_folder_path}/races.csv',header=True,schema=races_schema)

# COMMAND ----------

#checks for defininf the schema
raw_race_df.display()
raw_race_df.describe().display()
raw_race_df.printSchema()

# COMMAND ----------

race_col_renamed = raw_race_df.withColumnRenamed('raceId','race_id')\
                                   .withColumnRenamed('circuitId','circuit_id')\
                                   .withColumnRenamed('year','race_year')\
                                   .withColumn('race_timestamp', to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

race_col_with_ingestion_date = add_ingestion_date(race_col_renamed)

races_final_df = race_col_with_ingestion_date.drop('url', 'date', 'time')

races_final_df.display()
races_final_df.describe().display()
races_final_df.printSchema()

# COMMAND ----------

races_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/races')

# COMMAND ----------

#check parquet files
spark.read.parquet('/mnt/dlcoursestorage/processed/races').display()
display(dbutils.fs.ls ("/mnt/dlcoursestorage/processed/races"))
