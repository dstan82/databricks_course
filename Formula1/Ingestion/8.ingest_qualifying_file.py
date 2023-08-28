# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest qualifying JSON

# COMMAND ----------

dbutils.widgets.text('p_data_source','') # define widget
v_data_source = dbutils.widgets.get('p_data_source') # retrieve the parameter from the widget

print(v_data_source)

# COMMAND ----------

dbutils.widgets.text('p_file_date','2021-03-28') # define widget
v_file_date = dbutils.widgets.get('p_file_date') # retrieve the parameter from the widget

print(v_file_date)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.fs.mounts()
display(dbutils.fs.ls(f'{raw_folder_path}/{v_file_date}/qualifying'))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType

# COMMAND ----------

qualifying_schema = StructType([StructField('qualifyId',IntegerType(),False), \
                             StructField('raceId',IntegerType(),True), \
                             StructField('driverId',IntegerType(),True), \
                             StructField('constructorId',IntegerType(),True), \
                             StructField('number',IntegerType(),True), \
                             StructField('position',IntegerType(),True), \
                             StructField('q1',StringType(),True), \
                             StructField('q2',StringType(),True), \
                             StructField('q3',StringType(),True), \
])

# COMMAND ----------

qualifying_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/qualifying/qualifying_split_*.json',multiLine=True,schema=qualifying_schema)

# COMMAND ----------

qualifying_df.display()
qualifying_df.printSchema()
qualifying_df.describe().display()

# COMMAND ----------

from pyspark.sql.functions import lit

col_rename_qualifying_df = qualifying_df.withColumnRenamed('qualifyId','qualify_id') \
                      .withColumnRenamed('raceId','race_id') \
                      .withColumnRenamed('driverId','driver_id') \
                      .withColumnRenamed('constructorId','constructor_id') \
                      .withColumn('data_source', lit(v_data_source))
                          
final_qualifying_df = add_ingestion_date(col_rename_qualifying_df)

# COMMAND ----------

final_qualifying_df.display()

# COMMAND ----------

#final_qualifying_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying')

#function in 'includes' notebook - expects: Schema, Table, DataFrame, Partitioning_Column
incremental_load('f1_processed','qualifying',final_qualifying_df,'race_id')

# COMMAND ----------

dbutils.fs.ls('/mnt/dlcoursestorage/processed/qualifying')
spark.read.parquet('/mnt/dlcoursestorage/processed/qualifying').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*) FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

dbutils.notebook.exit('Success')
