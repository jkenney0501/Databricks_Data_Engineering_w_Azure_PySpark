# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake using SAS token
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope')

# COMMAND ----------

f1_sas = dbutils.secrets.get(scope='formula1-scope', key='sas-f1')

# COMMAND ----------

# add configuration for SAS token
spark.conf.set("fs.azure.account.auth.type.dlformula1jk .dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dlformula1jk .dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dlformula1jk.dfs.core.windows.net", f1_sas)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@dlformula1jk.dfs.core.windows.net"))


# COMMAND ----------

# read the circuits file with spark dataframe api
display(spark.read.csv("abfss://demo@dlformula1jk.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


