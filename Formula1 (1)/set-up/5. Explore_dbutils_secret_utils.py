# Databricks notebook source
# MAGIC %md
# MAGIC ## Explore the capabilities of the dbutils.secrets utility

# COMMAND ----------

# start with the help method
dbutils.secrets.help()

# COMMAND ----------

# list all secret scope available
dbutils.secrets.listScopes()

# COMMAND ----------

# using list, it expects the parameter "scope" to be passed in to show you the key 
dbutils.secrets.list(scope='formula1-scope')


# COMMAND ----------

# get the key
dbutils.secrets.get(scope='formula1-scope', key='formula1dl-acct-key')

# COMMAND ----------


