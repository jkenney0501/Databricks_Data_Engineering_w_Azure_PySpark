# Databricks notebook source
# MAGIC %md
# MAGIC ## Joins Transformations
# MAGIC
# MAGIC parameters: on=*primary key* how=*join type-inner, left, outer etc*
# MAGIC
# MAGIC See more in docs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in circuits file and races file from clean folder path
# MAGIC - these two will be joined with circuit_id 

# COMMAND ----------

# read the parquet 
# files in from the clean container
circuits_df = spark.read.parquet(f'{clean_folder_path}/circuits') \
    .withColumnRenamed("name", "circuit_name")

races_df = spark.read.parquet(f'{clean_folder_path}/races').filter("race_year =  2019") \
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

# view the races_df
races_df.show(3)

# COMMAND ----------

# get a row count - 21 records
races_df.count()

# COMMAND ----------

# view the circuits_df
circuits_df.show(3)

# COMMAND ----------

# get a row count for circuits - 77 rows
circuits_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inner Join on circuit_id

# COMMAND ----------

# the left table is circuits and we are inner joining the races table which will give us only results from the right table tha match the left table
circuits_races_df = circuits_df.join(races_df, circuits_df.circuit_id==races_df.circuit_id, how='inner') \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.race_country, races_df.race_name, races_df.round)

# COMMAND ----------

# view the joined table, the results are the selected columns from both tables. 21 rows total, only macthing results.
display(circuits_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Outer Join
# MAGIC - left outer provides all the data from the left table and all corresponding/matching rows from the right and where the rows dont match on the right, we see a null value in the right table.
# MAGIC - essentially, if the left table has 100 rows, we will get 100 rows returned. Nulls will be present in the right table where they dont match the left.

# COMMAND ----------

# alter the circuits table by filtering it for races up to 69
circuits_df2 = spark.read.parquet(f'{clean_folder_path}/circuits') \
    .filter("circuit_id < 70") \
    .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

# left join using the races table from above - 69 ciruit records with 21 race records. The larger table will be the left to show a left join.
circuits_races_df = circuits_df2.join(races_df, circuits_df2.circuit_id==races_df.circuit_id, how='left') \
    .select(circuits_df2.circuit_name, circuits_df2.location, circuits_df2.race_country, races_df.race_name, races_df.round)

# COMMAND ----------

# you can see the nulls in rows 2 and 5 where there are no matching records to the circuits table.
display(circuits_races_df)

# COMMAND ----------

# there are a total of 51 records that dont match!
circuits_races_df.where("round is null and race_name is null").count()

# COMMAND ----------

circuits_races_df.show(69, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right Join
# MAGIC - works in the opposit of the left.
# MAGIC - nulls on left table where theres no match to the right

# COMMAND ----------

circuits_races_df = circuits_df2.join(races_df, circuits_df2.circuit_id==races_df.circuit_id, how='right') \
    .select(circuits_df2.circuit_name, circuits_df2.location, circuits_df2.race_country, races_df.race_name, races_df.round)

# COMMAND ----------

circuits_races_df.show(70, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Outer Join
# MAGIC - a FOJ is acombination of both left and right with respoect to non matching rows. You will see nulls on both sids where there is no match.

# COMMAND ----------

circuits_races_df = circuits_df2.join(races_df, circuits_df2.circuit_id==races_df.circuit_id, how='full') \
    .select(circuits_df2.circuit_name, circuits_df2.location, circuits_df2.race_country, races_df.race_name, races_df.round)

# COMMAND ----------

# you will get more rows with a full outer due to the combination
circuits_races_df.count()

# COMMAND ----------

circuits_races_df.show(70, False)

# COMMAND ----------


