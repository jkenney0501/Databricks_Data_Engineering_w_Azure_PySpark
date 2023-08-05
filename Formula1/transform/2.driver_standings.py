# Databricks notebook source
# MAGIC %md
# MAGIC ## Produce Driver Standings 
# MAGIC - Using results DF in clean container

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# read in the results files in the clean container
race_results_df = spark.read.parquet(f'{final_folder_path}/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# create a new df that groups the year, driver name, team and then sums total points and counts only where position == 1 (wins) then rename the column as "wins"
from pyspark.sql.functions import sum, when, col, count

driver_standings = race_results_df \
    .groupBy('race_year', 'driver_nationality', 'driver_name', 'team') \
    .agg(sum('points').alias('total_points'), count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

display(driver_standings)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rank the Drivers

# COMMAND ----------

from pyspark.sql.functions import desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

driver_rank_df = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))

final_df = driver_standings.withColumn('rank', rank().over(driver_rank_df))

# COMMAND ----------

display(final_df.filter('race_year = 2020'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the Rankings to a Parquet file in the final container

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_final.driver_standings')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from f1_final.driver_standings;

# COMMAND ----------


