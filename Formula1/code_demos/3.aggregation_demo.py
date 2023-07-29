# Databricks notebook source
# MAGIC %md
# MAGIC ## Aggregate Functions

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# read in the race results file from the clean container
race_results_df = spark.read.parquet(f'{final_folder_path}/race_results')

# COMMAND ----------

# view the df
display(race_results_df)

# COMMAND ----------

# filter the df to 2020 for a smaller df to work with - the current on eis quite large
demo_df = race_results_df.filter('race_year == 2020')

# COMMAND ----------

# 340 records
demo_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Built in Aggregate Functions

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Count Distinct

# COMMAND ----------

# get a distinct count on a column
demo_df.select(countDistinct('race_name').alias('race_name_count')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sum

# COMMAND ----------

# sum the points for 2020
demo_df.select(sum('points').alias('total_points')).show()

# COMMAND ----------

# get total points for lewis hamilton and a count of distinct races
demo_df.filter("driver_name == 'Lewis Hamilton' ") \
    .select(sum('points').alias('total_points'), \
    countDistinct('race_name').alias('total_races')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group Aggregations

# COMMAND ----------

# get total points for each driver in 2020
demo_df.groupBy('driver_name') \
    .sum('points') \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply more than one aggregation across a group

# COMMAND ----------

demo_df.groupBy('driver_name') \
    .agg(sum('points').alias('total_points'), countDistinct('race_name').alias('races')) \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Functions
# MAGIC
# MAGIC **Steps:**
# MAGIC - Create a group 
# MAGIC - Create the window specs: partition by and order by columns in a new df
# MAGIC - Create a new df with the window function over(pass in the above window spec df)
# MAGIC
# MAGIC Docs for Window: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html
# MAGIC
# MAGIC Docs for Functions: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#window-functions

# COMMAND ----------

# create a new data frame with 2 years of race data to make more applicable to window functions
demo_df_wf = race_results_df.filter('race_year in (2019, 2020)')

# COMMAND ----------

# 760 rows
display(demo_df_wf)

# COMMAND ----------

# create a grouped df that is grouped on year and driver name which we will rank later
demo_grouped_df = demo_df_wf.groupBy('race_year', 'driver_name') \
                 .agg(sum('points').alias('total_points'), countDistinct('race_name').alias('races')) 

# COMMAND ----------

display(demo_grouped_df.sort('race_year'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rank

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# create the window specification
driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'))

# create the rankeand pass the window spec above to it
demo_grouped_df.withColumn('rank', rank().over(driver_rank_spec)).show(50)

# COMMAND ----------


