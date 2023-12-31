# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text('p_data_source','') # define widget
v_data_source = dbutils.widgets.get('p_data_source') # retrieve the parameter from the widget

print(v_data_source)

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-21') # define widget (widget name, default value)
v_file_date = dbutils.widgets.get('p_file_date') # retrieve the parameter from the widget and store it in a variable

print(v_file_date)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

constructors_schema = "constructorId INT,constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

dbutils.fs.mounts()
dbutils.fs.ls(f'{raw_folder_path}')

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop unwanted column in the dataframe

# COMMAND ----------

constructors_drop_url_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import lit

constructor_col_rename_df = constructors_drop_url_df.withColumnRenamed('constructorId','constructor_id') \
                                               .withColumnRenamed('constructorRef','constructor_ref') \
                                               .withColumn('data_source', lit(v_data_source)) \
                                               .withColumn('file_date',lit(v_file_date))
                                            
constructor_final_df = add_ingestion_date(constructor_col_rename_df)

# COMMAND ----------

constructor_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.constructors')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dlcoursestorage/processed/constructors'))
spark.read.parquet('/mnt/dlcoursestorage/processed/constructors').display()

# COMMAND ----------

dbutils.notebook.exit('Success')
