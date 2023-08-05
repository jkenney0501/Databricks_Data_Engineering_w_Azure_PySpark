-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_final
LOCATION '/mnt/dlformula1jk/final'

-- COMMAND ----------

-- see the description of the db
DESC DATABASE f1_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Go to ALL final files, rewrite to format as parquet and to to save as table: f1_clean.<folder_name> and delete the folders via storage explore and run the notebook again. They will get recreated and we can use sql or python now.

-- COMMAND ----------


