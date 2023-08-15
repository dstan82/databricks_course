# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest constructors.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT,constructorRef STRING, name STRING, nationality string, url STRING"

# COMMAND ----------

dbutils.fs.mounts()
dbutils.fs.ls(f'{raw_folder_path}')

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json(f'{raw_folder_path}/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop unwated column in the dataframe

# COMMAND ----------

constructors_drop_url_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 3 - Rename columns and add ingestion date

# COMMAND ----------

constructor_col_rename_df = constructors_drop_url_df.withColumnRenamed('constructorId','constructor_id') \
                                               .withColumnRenamed('constructorRef','constructor_ref') \
                                            
constructor_final_df = add_ingestion_date(constructor_col_rename_df)

# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/constructors')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dlcoursestorage/processed/constructors'))
spark.read.parquet('/mnt/dlcoursestorage/processed/constructors').display()
