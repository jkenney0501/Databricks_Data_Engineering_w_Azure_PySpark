# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Pit Stops JSON File
# MAGIC *Pitstops is a multi-line json file which is handled a little different - spark need to be told explicitly that it is mutli-line json*
# MAGIC
# MAGIC **Requirements:**
# MAGIC
# MAGIC 1. Rename columns raceId and driverId.
# MAGIC 2. Add ingestion date column.
# MAGIC 3. Write to clean container as parquet.

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# import required types for schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# create schema to enforce
pit_stop_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('stop', IntegerType(), False),
    StructField('lap', IntegerType(), False),
    StructField('time', StringType(), False),
    StructField('duration', StringType(), False),
    StructField('milliseconds', StringType(), False)
])

# COMMAND ----------

# read in the data-specify multiLine=True
pitstop_df = spark.read \
    .schema(pit_stop_schema) \
    .option('multiLine', True) \
    .json(f'{stage_folder_path}/pit_stops.json')

# COMMAND ----------

# read in the df
display(pitstop_df)

# COMMAND ----------

# import current_timestamp for new column to be added
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# rename columns raceId and driverId
pit_stop_df_renamed = pitstop_df.withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# view the new df
pit_stop_df_renamed.show(3, truncate=False)

# COMMAND ----------

# write the df to the clean container
pit_stop_df_renamed.write.mode('overwrite').parquet(f'{clean_folder_path}/pit_stops')

# COMMAND ----------

# view the parquet in the clean container
display(spark.read.parquet(f'{clean_folder_path}/pit_stops'))

# COMMAND ----------


