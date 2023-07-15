# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake using Cluster Scope Credentials
# MAGIC 1. Compute --> advanced--> edit--> Set the spark config: fs.azure.account.key.dlformual1jk.dfs.core.windows.net {{secret/formuala1-scope/formala1dl-acct-key}}
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlformula1jk.dfs.core.windows.net"))

# COMMAND ----------

# read the circuits file with spark dataframe api
display(spark.read.csv("abfss://demo@dlformula1jk.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


