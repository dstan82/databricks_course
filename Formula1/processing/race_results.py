# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

#reading parquet files from datalake and creating dataframes
circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')
drivers_df = spark.read.parquet(f'{processed_folder_path}/drivers')
constructors_df = spark.read.parquet(f'{processed_folder_path}/constructors')
races_df = spark.read.parquet(f'{processed_folder_path}/races')
results_df = spark.read.parquet(f'{processed_folder_path}/results')

# COMMAND ----------

#renaming columns to reflect the dataset they are comming from
circuits_rename_df = circuits_df.withColumnRenamed('name','circuit_name').withColumnRenamed('location','circuit_location')
drivers_rename_df = drivers_df.withColumnRenamed('name','driver_name').withColumnRenamed('number','driver_number').withColumnRenamed('nationality','driver_nationality')
constructors_rename_df = constructors_df.withColumnRenamed('name','team')
races_rename_df = races_df.withColumnRenamed('name','race_name').withColumnRenamed('race_timestamp','race_date')
results_rename_df = results_df.withColumnRenamed('time','race_time')

# COMMAND ----------

#check the content
circuits_rename_df.display()
drivers_rename_df.display()
constructors_rename_df.display()
races_rename_df.display()
results_rename_df.display()
print(results_rename_df.count())

# COMMAND ----------

#joining all datasets
presentation_df = results_rename_df.join(races_rename_df,'race_id','inner')\
                            .join(constructors_rename_df, 'constructor_id','inner')\
                            .join(drivers_rename_df, 'driver_id','inner')\
                            .join(circuits_rename_df, 'circuit_id','inner')

# COMMAND ----------

#select only required fields
presentation_select_df = presentation_df.select(presentation_df.race_year\
                                                ,presentation_df.race_name\
                                                ,presentation_df.race_date\
                                                ,presentation_df.circuit_location\
                                                ,presentation_df.driver_name\
                                                ,presentation_df.driver_number\
                                                ,presentation_df.driver_nationality\
                                                ,presentation_df.team\
                                                ,presentation_df.grid\
                                                ,presentation_df.fastest_lap\
                                                ,presentation_df.race_time\
                                                ,presentation_df.points)
presentation_select_df.display()
print(presentation_select_df.count())

# COMMAND ----------

#adding creation date
presentation_and_date_df = add_current_timestamp(presentation_select_df,'created_date')

presentation_and_date_df.display()
print(presentation_and_date_df.count())

# COMMAND ----------

presentation_and_date_df.write.mode('overwrite').parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

#read back and test
spark.read.parquet(f'{presentation_folder_path}/race_results').count()
display(dbutils.fs.ls(f'{presentation_folder_path}/race_results'))

# COMMAND ----------

#check the results against existing report
report = spark.read.parquet(f'{presentation_folder_path}/race_results')

report.filter((report.race_year == 2020) & (report.circuit_location == 'Abu Dhabi')).orderBy(report.points.desc()).display()

