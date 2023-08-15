# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest circuits.csv file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read the CSV file using thwe spark dataframe reader

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

display(dbutils.fs.ls(raw_folder_path))

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

circuits_df = spark.read.csv(f'{raw_folder_path}/circuits.csv',header=True,schema=circuits_schema)

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - Select only the required columns

# COMMAND ----------

#Method1 - only selects the given columns
#circuits_selected_df = circuits_df.select('circuitId', 'circuitRef', 'name', 'location', 'country', 'lat', 'lng', 'alt')

#Method2 - selects & functions can be applied (ie. alias/rename)
#circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

#Method3 - selects & functions can be applied (ie. alias/rename)
#circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"],circuits_df["lng"], circuits_df["alt"])

#Method4 - selects & functions can be applied (ie. alias/rename)
from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt'))


# COMMAND ----------

circuits_selected_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Reaname columns as required

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed('circuitId','circuit_id')\
    .withColumnRenamed('circuitRef','circuit_ref')\
    .withColumnRenamed('lat','latitude')\
    .withColumnRenamed('lng','longitude')\
    .withColumnRenamed('alt','altitude')

# COMMAND ----------

circuits_renamed_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

#from pyspark.sql.functions import current_timestamp
#from pyspark.sql.functions import *

#circuits_final_df = circuits_renamed_df.withColumn('ingestion_date',current_timestamp())

circuits_final_df = add_ingestion_date(circuits_renamed_df)


# COMMAND ----------

circuits_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 5 - Write data to datalake as parquet

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

circuits_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/circuits')


# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/dlcoursestorage/processed/circuits"

# COMMAND ----------

df = spark.read.parquet(f'{processed_folder_path}/circuits')

# COMMAND ----------

df.display()
