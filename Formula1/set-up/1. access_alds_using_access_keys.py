# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake using Access Keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

# get secrets from secret scope and azure key vault
f1_acct_key = dbutils.secrets.get(scope='formula1-scope', key='formula1dl-acct-key')

# COMMAND ----------

# configure spark with key
spark.conf.set(
    "fs.azure.account.key.dlformula1jk.dfs.core.windows.net",
    f1_acct_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlformula1jk.dfs.core.windows.net"))

# COMMAND ----------

# read the circuits file with spark dataframe api
display(spark.read.csv("abfss://demo@dlformula1jk.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


