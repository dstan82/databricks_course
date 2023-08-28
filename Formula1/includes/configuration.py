# Databricks notebook source
#display(dbutils.fs.mounts())

# COMMAND ----------



# COMMAND ----------

#with mount (premium)
raw_folder_path = '/mnt/dlcoursestorage/raw'
processed_folder_path = '/mnt/dlcoursestorage/processed'
presentation_folder_path = '/mnt/dlcoursestorage/presentation'
demo_folder_path = '/mnt/dlcoursestorage/demo'

# COMMAND ----------

#without active directory (no mounts)
'''
raw_folder_path = 'abfss://raw@dlcoursestorage.dfs.core.windows.net/'
processed_folder_path = 'abfss://processed@dlcoursestorage.dfs.core.windows.net/'
presentation_folder_path = 'abfss://presentation@dlcoursestorage.dfs.core.windows.net/'
'''
