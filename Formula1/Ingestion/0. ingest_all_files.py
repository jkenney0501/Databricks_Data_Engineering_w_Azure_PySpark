# Databricks notebook source
# see the dbutils for notebook
dbutils.notebook.help()

"""
The notebook module.
exit(value: String): void -> This method lets you exit a notebook with a value
run(path: String, timeoutSeconds: int, arguments: Map): String -> This method runs a notebook and returns its exit value
"""

# COMMAND ----------

# run a notebook as an example - click link to see the job (it ran successfully), if this fails, you have no way of knowing.
v_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

# add the other notebooks
v_result1 = dbutils.notebook.run("2. Ingest_races_csv_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result1 = dbutils.notebook.run("3. ingestion_constructors_json", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result1 = dbutils.notebook.run("4. ingest_drivers_json_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result1 = dbutils.notebook.run("5. ingest_results_json_file", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result1 = dbutils.notebook.run("6. ingest_pit_stops_multi_line_json", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result1 = dbutils.notebook.run("7. ingest_lap_times_folder", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result1 = dbutils.notebook.run("8. ingest_qualifying_folder_mutlti_json", 0, {"p_data_source":"Ergast API"})

# COMMAND ----------

v_result
