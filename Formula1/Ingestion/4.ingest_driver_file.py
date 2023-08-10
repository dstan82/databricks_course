# Databricks notebook source
# MAGIC %md
# MAGIC ##Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1 - Read JSON using spark dataframe reader API

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

drivers_df = spark.read.json('/mnt/dlcoursestorage/raw/drivers.json',schema=drivers_schema)
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

drivers_with_columns_df = drivers_df.withColumnRenamed('driverId', 'driver_id') \
                                .withColumnRenamed('driverRef','driver_ref') \
                                .withColumn('ingestion_date', current_timestamp()) \
                                .withColumn('name',concat('name.forename',lit(' '),'name.surname'))

# COMMAND ----------

drivers_with_columns_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 3 - Drop unwanted columns:
# MAGIC - name.forename
# MAGIC - name.surname
# MAGIC - url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step4 - Write the df to datalake in parquet format

# COMMAND ----------

drivers_final_df.write.parquet('/mnt/dlcoursestorage/processed/drivers')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dlcoursestorage/processed/drivers'))
spark.read.parquet('/mnt/dlcoursestorage/processed/drivers').display()
