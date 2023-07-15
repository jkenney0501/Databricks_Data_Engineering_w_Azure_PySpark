# Databricks notebook source
# MAGIC %md
# MAGIC ## Explore DBFS Root
# MAGIC 1. List all the folders sin the DBFS root
# MAGIC 2. Interact with the DBFS file browser.
# MAGIC 3. Upload a folder to DBFS.
# MAGIC
# MAGIC **Note:** FilesStore is accessiblle by all users that have access to the workspace. Be cautious with the data! There is no user level control like blob storage.

# COMMAND ----------

# explore the root directory
display(dbutils.fs.ls('/'))

# COMMAND ----------

# list all files in the sample data directory
display(dbutils.fs.ls('dbfs:/databricks-datasets/'))

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/databricks-datasets/airlines/part-00001'))

# COMMAND ----------

display(spark.read.csv('dbfs:/databricks-datasets/airlines/part-00001'))

# COMMAND ----------

# to enable the dbfs filestore: upper right corner-->admin settings-->workspace setting-->enable dbfs browser-->then go to to data-->click browse dbfs--> filestore is upper left. You can upload files from here. Click upload. --circuits.csv file

# COMMAND ----------

# run the dbutils again to see the new file loaded 
display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

# display the new file
display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------


