# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step1 - Read the CSV file using thwe spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dlcoursestorage/raw'))

# COMMAND ----------

circuits_schema = StructType(fields=[StructField('circuitId', IntegerType(),False),
                                     StructField('circuitRef', StringType(),True),
                                     StructField('name', StringType(),True),
                                     StructField('location', StringType(),True),
                                     StructField('country', StringType(),True),
                                     StructField('lat', DoubleType(),True),
                                     StructField('lng', DoubleType(),True),
                                     StructField('alt', IntegerType(),True),
                                     StructField('url', StringType(),True)
])

# COMMAND ----------

circuits_df = spark.read.csv('/mnt/dlcoursestorage/raw/circuits.csv',header=True,schema=circuits_schema)

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()
