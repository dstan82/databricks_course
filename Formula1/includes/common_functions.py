# Databricks notebook source
#Adding a column called 'ingestion_Date' with current timestamp

def add_ingestion_date (input_df):
    from pyspark.sql.functions import current_timestamp
    output_df = input_df.withColumn('ingestion_date',current_timestamp())
    return output_df

# COMMAND ----------

#Adding a column with current timestamp and function also takes in the column name (same function as above...)

def add_current_timestamp (input_df,column_name):
    from pyspark.sql.functions import current_timestamp
    output_df = input_df.withColumn(column_name,current_timestamp())
    return output_df

# COMMAND ----------

#Function for moving the partition column to last position - required for incremental load

def move_partition_column(df,partition_column_name):
    in_columns = df.schema.names
    partition_column_index = in_columns.index(partition_column_name)
    out_columns = in_columns[:partition_column_index]+in_columns[partition_column_index+1:]+in_columns[partition_column_index:partition_column_index+1]
    return df.select(out_columns)

# COMMAND ----------

#Function for incremental load. First calls the function above for changinf the partition column, then if the table exists it overwrites the partitions from DF to the existing {schema}.{table}

def incremental_load(schema, table,df, partition_column):
    df = move_partition_column(df,partition_column)
    spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')
    if (spark._jsparkSession.catalog().tableExists(f'{schema}.{table}')):
        df.write.mode('overwrite').insertInto(f'{schema}.{table}')
    else:
        df.write.mode('overwrite').partitionBy(partition_column).format('parquet').saveAsTable(f'{schema}.{table}')
