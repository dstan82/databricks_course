# Databricks notebook source
def add_ingestion_date (input_df):
    from pyspark.sql.functions import current_timestamp
    output_df = input_df.withColumn('ingestion_date',current_timestamp())
    return output_df

# COMMAND ----------

def add_current_timestamp (input_df,column_name):
    from pyspark.sql.functions import current_timestamp
    output_df = input_df.withColumn(column_name,current_timestamp())
    return output_df
