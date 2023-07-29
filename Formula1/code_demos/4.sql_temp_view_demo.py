# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Dataframes Using SQL
# MAGIC
# MAGIC **Objectives**
# MAGIC 1. Create temporary views on dataframes.
# MAGIC 2. Access the view from the SQL cell.
# MAGIC 3. Access the view the the python cell.

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

#read in the race results df in the final container
race_results_df = spark.read.parquet(f'{final_folder_path}/race_results')

# COMMAND ----------

# quick view of first 3 rows
race_results_df.show(3, False)

# COMMAND ----------

# create a temp view and name it
race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access the view with SQL
# MAGIC - Using the %sql magic command, you can use SQL like you would using any relational database.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(1)
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access using spark sql in a python cell

# COMMAND ----------

spark.sql("""
          SELECT * 
          FROM v_race_results
          WHERE race_year = 2018
          AND team IN('Mercedes')
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using an f string inside a sql statement

# COMMAND ----------

rc_yr = 2019

spark.sql(f"SELECT * FROM v_race_results WHERE race_year = {rc_yr}").show()

# COMMAND ----------


