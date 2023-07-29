# Databricks notebook source
# MAGIC %md
# MAGIC ## Filtering data in spark

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# using the races table 
races_df = spark.read.parquet(f'{clean_folder_path}/races')

# COMMAND ----------

# view the data
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter by year

# COMMAND ----------

race_year_filtered = races_df.filter('race_year = 2019')

display(race_year_filtered)

# COMMAND ----------

# you can also filter using the method chaining/pythonic way
race_year_filtered = races_df.filter(races_df.race_year == 2017)

display(race_year_filtered)

# COMMAND ----------

# filltering multiple conditions - uses parenthese for separate conditions and separate with &
race_year_filtered = races_df.filter((races_df.race_year == 2019) & (races_df.round <= 5))

display(race_year_filtered)

# COMMAND ----------

# using the sql style - you can use "and" with sql style inisde of quotes
race_year_filtered = races_df.filter('race_year = 2019 and round <= 5')

display(race_year_filtered)

# COMMAND ----------


