# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Introduction
# MAGIC
# MAGIC ## UI Introduction
# MAGIC ## Magic Commands
# MAGIC
# MAGIC - %Python
# MAGIC - %sql
# MAGIC - %md

# COMMAND ----------

message = "Welcome to databricks notebooks"

# COMMAND ----------

print(message)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT NOW()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/databricks-datasets/

# COMMAND ----------

# MAGIC %sh
# MAGIC ps
