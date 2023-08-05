# Databricks notebook source
# MAGIC %md
# MAGIC ## Race Results for Presentation
# MAGIC
# MAGIC **Requirements:**
# MAGIC
# MAGIC - Join races, circuits, drivers, constructors and results to create a table that shows a summary of race results.
# MAGIC - add created_date as current_timestamp()
# MAGIC - write as parquet to final container which is the presentation layer
# MAGIC
# MAGIC **Columns:**
# MAGIC - race_year : **Source**-races
# MAGIC - race_name : **Source**-races
# MAGIC - race_date : **Source**-races
# MAGIC - circuit_location : **Source**-circuits
# MAGIC - driver_name : **Source**-drivers
# MAGIC - driver_number : **Source**-drivers
# MAGIC - driver_nationality : **Source**-drivers
# MAGIC - team : **Source**-constructors
# MAGIC - grid : **Source**-results
# MAGIC - fastest lap : **Source**-results
# MAGIC - race time : **Source**-results
# MAGIC - points : **Source**-results
# MAGIC - created_date : **Source**-current_timestamp()

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read in all the tables as data frames from the clean container

# COMMAND ----------

# Rename "name" columns to avoid conflicts: be specific to the table - for example, name is in drivers, races and circuits 
drivers_df = spark.read.parquet(f'{clean_folder_path}/drivers') \
    .withColumnRenamed('name', 'driver_name') \
    .withColumnRenamed('number', 'driver_number') \
    .withColumnRenamed('nationality', 'driver_nationality') 

# COMMAND ----------

races_df = spark.read.parquet(f'{clean_folder_path}/races') \
    .withColumnRenamed('name', 'race_name') \
    .withColumnRenamed('race_timestamp', 'race_date') 

# COMMAND ----------

circuit_df = spark.read.parquet(f'{clean_folder_path}/circuits') \
    .withColumnRenamed('name', 'circuits_name') \
    .withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

results_df = spark.read.parquet(f'{clean_folder_path}/results') \
    .withColumnRenamed('time', 'race_time')

# COMMAND ----------

constructors_df = spark.read.parquet(f'{clean_folder_path}/constructors') \
    .withColumnRenamed('name', 'team')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join the dataframes
# MAGIC - join the circuits df to the races df fist on circuit_id since circuits doesnt have another key that links it to any other table that we are using.

# COMMAND ----------

race_circuits_df = races_df.join(circuit_df, races_df.circuit_id == circuit_df.circuit_id, 'inner') \
    .select('race_id','race_year', 'race_name', 'race_date', 'circuit_location')

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join results to other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id==race_circuits_df.race_id, 'inner') \
    .join(drivers_df, results_df.driver_id==drivers_df.driver_id, 'inner') \
    .join(constructors_df, results_df.constructor_id==constructors_df.constructor_id, 'inner')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# select the needed columns
final_df =  race_results_df.select('race_year', 'race_name', 'race_date', 'circuit_location', 
                                   'driver_name', 'driver_number', 'driver_nationality', 'team', 
                                   'grid', 'fastest_lap', 'race_time', 'points', 'position').withColumn('created_date', current_timestamp())

# COMMAND ----------

# matching results to BBC website for 2020 results
display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix' ").orderBy(final_df.points.desc()))

# COMMAND ----------

final_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the dataframe to the final container

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_final.race_results')

# COMMAND ----------

# read in the parquet from final
display(spark.read.parquet(f'{final_folder_path}/race_results'))

# COMMAND ----------


