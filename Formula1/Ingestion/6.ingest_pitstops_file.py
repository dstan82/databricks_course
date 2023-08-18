# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest pitstops JSON

# COMMAND ----------

dbutils.widgets.text('p_data_source','') # define widget
v_data_source = dbutils.widgets.get('p_data_source') # retrieve the parameter from the widget

print(v_data_source)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.fs.mounts()
dbutils.fs.ls('/mnt/dlcoursestorage/raw')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType

# COMMAND ----------

pit_stops_schema = StructType([StructField('driverId',IntegerType(),False), \
                             StructField('duration',StringType(),True), \
                             StructField('lap',IntegerType(),True), \
                             StructField('milliseconds',IntegerType(),True), \
                             StructField('raceId',IntegerType(),True), \
                             StructField('stop',IntegerType(),True), \
                             StructField('time',StringType(),True)
])

# COMMAND ----------

pitstops_df = spark.read.json(f'{raw_folder_path}/pit_stops.json',multiLine=True,schema=pit_stops_schema)

# COMMAND ----------

pitstops_df.display()
pitstops_df.printSchema()
pitstops_df.describe().display()

# COMMAND ----------

from pyspark.sql.functions import lit

pit_stops_col_rename_df = pitstops_df.withColumnRenamed('driverId','driver_id') \
                      .withColumnRenamed('raceId','race_id') \
                        .withColumn('data_source',lit(v_data_source))
                    

final_pit_stops_df = add_ingestion_date(pit_stops_col_rename_df)

# COMMAND ----------

final_pit_stops_df.display()

# COMMAND ----------

final_pit_stops_df.write.mode('overwrite').parquet(f'{processed_folder_path}/pitstops')

# COMMAND ----------

dbutils.fs.ls('/mnt/dlcoursestorage/processed/pitstops')
spark.read.parquet('/mnt/dlcoursestorage/processed/pitstops').display()

# COMMAND ----------

dbutils.notebook.exit('Success')
