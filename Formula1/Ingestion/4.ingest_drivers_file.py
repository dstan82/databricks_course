# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - Read JSON using spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text('p_data_source','') # define widget
v_data_source = dbutils.widgets.get('p_data_source') # retrieve the parameter from the widget

print(v_data_source)

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

#dbutils.fs.mounts()
#dbutils.fs.ls('/mnt/dlcoursestorage/raw/')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType([StructField('forename',StringType(),True), \
                          StructField('surname',StringType(),True), \
])

drivers_schema = StructType([StructField('code',StringType(), False),\
                             StructField('dob',DateType(), True),\
                             StructField('driverId',IntegerType(), True),\
                             StructField('driverRef',StringType(), True), \
                             StructField('name',name_schema), \
                             StructField('nationality',StringType(), True), \
                             StructField('number',IntegerType(), True), \
                             StructField('url',StringType(), True)
    ])

# COMMAND ----------

drivers_df = spark.read.json(f'{raw_folder_path}/drivers.json',schema=drivers_schema)
drivers_df.display()
drivers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 2 - Rename columns and add new column
# MAGIC
# MAGIC 1. driverId to driver_id
# MAGIC 2. driverRef to driver_ref
# MAGIC 3. add ingestion date
# MAGIC 4. name added with concatenation of forename and surname 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit,col

# COMMAND ----------

from pyspark.sql.functions import lit

drivers_with_columns_df = drivers_df.withColumnRenamed('driverId', 'driver_id') \
                                .withColumnRenamed('driverRef','driver_ref') \
                                .withColumn('name',concat('name.forename',lit(' '),'name.surname'))\
                                .withColumn('data_source', lit(v_data_source))
drivers_with_ingestion_date = add_ingestion_date(drivers_with_columns_df)

# COMMAND ----------

drivers_with_ingestion_date.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Drop unwanted columns:
# MAGIC - name.forename
# MAGIC - name.surname
# MAGIC - url

# COMMAND ----------

drivers_final_df = drivers_with_ingestion_date.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step4 - Write the df to datalake in parquet format

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/drivers')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dlcoursestorage/processed/drivers'))
spark.read.parquet('/mnt/dlcoursestorage/processed/drivers').display()

# COMMAND ----------

dbutils.notebook.exit('Success')
