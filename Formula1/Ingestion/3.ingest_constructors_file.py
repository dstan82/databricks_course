# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest constructors.json file

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_schema = "constructorId INT,constructorRef STRING, name STRING, nationality string, url STRING"

# COMMAND ----------

dbutils.fs.mounts()
dbutils.fs.ls('/mnt/dlcoursestorage/raw')

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json('/mnt/dlcoursestorage/raw/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop unwated column in the dataframe

# COMMAND ----------

constructors_drop_url_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 3 - Rename columns and add ingestion date

# COMMAND ----------

constructor_final_df = constructors_drop_url_df.withColumnRenamed('constructorId','constructor_id') \
                                               .withColumnRenamed('constructorRef','constructor_ref') \
                                               .withColumn('ingestion_date',current_timestamp())

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet('/mnt/dlcoursestorage/processed/constructors')

# COMMAND ----------

#display(dbutils.fs.ls('/mnt/dlcoursestorage/processed/constructors'))
#spark.read.parquet('/mnt/dlcoursestorage/processed/constructors').display()
