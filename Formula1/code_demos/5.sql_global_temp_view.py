# Databricks notebook source
# MAGIC %md
# MAGIC ## Global Temporary Views
# MAGIC - Global temp views are available to all notebooks attached to the cluster as opposed to Temp views which are session based (aka that notebook only)
# MAGIC - When to use a global temp view vs a local temp view? Use local when the scope is just a notebook and global when there are other notebooks invlolved in the project and the view will be referenced.
# MAGIC
# MAGIC **Objectives**
# MAGIC 1. Create global temporary views on dataframes.
# MAGIC 2. Access the view from the SQL cell.
# MAGIC 3. Access the view the the python cell.
# MAGIC 4. Access the view from another notebook.

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
race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access the global temp view with SQL
# MAGIC - Using the %sql magic command, you can use SQL like you would using any relational database.
# MAGIC
# MAGIC *Spark registers a global temp view under a database called global_temp, to access a global temp view you must qualify the table with* **global_temp** 
# MAGIC
# MAGIC To see the databases: %sql SHOW DATABASE 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(1)
# MAGIC FROM global_temp.gv_race_results
# MAGIC WHERE race_year = 2020;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access using spark sql in a python cell

# COMMAND ----------

spark.sql("""
          SELECT * 
          FROM global_temp.gv_race_results
          WHERE race_year = 2018
          AND team IN('Mercedes')
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using an f string inside a sql statement

# COMMAND ----------

rc_yr = 2019

spark.sql(f"SELECT * FROM global_temp.gv_race_results WHERE race_year = {rc_yr}").show()

# COMMAND ----------

# ctrl click the sql global temp view nptebook in workspace to open another tab and test out using a global temp view query from another notebook. Its rather simple and works without issues.

# COMMAND ----------


