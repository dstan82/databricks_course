# Databricks notebook source
# MAGIC %md
# MAGIC ###Step1 - Read the CSV file using thwe spark dataframe reader

# COMMAND ----------

#importing functions
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Checking mount path
# dbutils.fs.mounts()

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

raw_race_df = spark.read.csv('/mnt/dlcoursestorage/raw/races.csv',header=True,schema=races_schema)

# COMMAND ----------

#checks for defininf the schema
raw_race_df.display()
raw_race_df.describe().display()
raw_race_df.printSchema()

# COMMAND ----------

#drop URL
#race_selected_df = raw_race_df.select(col('raceId'), col('year'), col('round'), col('circuitId'), col('name'), col('date'), col('time'))
#race_selected_df.display()

# COMMAND ----------

race_col_renamed = race_selected_df.withColumnRenamed('raceId','race_id')\
                                   .withColumnRenamed('circuitId','circuit_id')\
                                   .withColumnRenamed('year','race_year')\
                                   .withColumn('race_timestamp', to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))\
                                   .withColumn('ingestion_date',current_timestamp())

races_final_df = race_col_renamed.drop('url', 'date', 'time')

races_final_df.display()
races_final_df.describe().display()
races_final_df.printSchema()

# COMMAND ----------

races_final_df.write.mode('overwrite').parquet('/mnt/dlcoursestorage/processed/races')

# COMMAND ----------

#check parquet files
spark.read.parquet('/mnt/dlcoursestorage/processed/races').display()
display(dbutils.fs.ls ("/mnt/dlcoursestorage/processed/races"))
