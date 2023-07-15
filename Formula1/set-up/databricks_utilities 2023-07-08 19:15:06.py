# Databricks notebook source
# MAGIC %md
# MAGIC ## File System Magic Commands

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC **db calls a db utils package - dbutils.fs to get the results**

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/

# COMMAND ----------

# call it direclty
display(dbutils.fs.ls('/databricks-datasets/'))

# COMMAND ----------

# MAGIC %md
# MAGIC Combine db untils with python to get only filenames.

# COMMAND ----------

for files in dbutils.fs.ls('/databricks-datasets/COVID'):
    if files.name.endswith('/'):
        print(files.name)
    

# COMMAND ----------

# MAGIC %md
# MAGIC See all the untils available with help()

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help('rm')
