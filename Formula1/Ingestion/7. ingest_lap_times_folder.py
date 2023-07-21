# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Lap Times Folder (multiple split files)
# MAGIC *Lap times is a folder with 5 split csv files*
# MAGIC
# MAGIC **Requirements:**
# MAGIC
# MAGIC 1. Rename columns raceId and driverId.
# MAGIC 2. Add ingestion date column.
# MAGIC 3. Write to clean container as parquet.

# COMMAND ----------

# import required types for schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# create schema to enforce
lap_times_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('lap', IntegerType(), False),
    StructField('position', IntegerType(), False),
    StructField('time', StringType(), False),
    StructField('milliseconds', StringType(), False)
])

# COMMAND ----------

# read in the data-specify the folder with all the csv files in it
lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv('/mnt/dlformula1jk/stage/lap_times/lap_times_split*.csv')

# COMMAND ----------

# read in the df
display(lap_times_df)

# COMMAND ----------

# import current_timestamp for new column to be added
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# rename columns raceId and driverId
lap_times_df_renamed = lap_times_df.withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# view the new df
lap_times_df_renamed.show(3, truncate=False)

# COMMAND ----------

# count the records - 490,904
lap_times_df_renamed.count()

# COMMAND ----------

# write the df to the clean container
lap_times_df_renamed.write.mode('overwrite').parquet('/mtn/dlformula1jk/clean/lap_times')

# COMMAND ----------

# view the parquet in the clean container
display(spark.read.parquet('/mtn/dlformula1jk/clean/lap_times'))

# COMMAND ----------


