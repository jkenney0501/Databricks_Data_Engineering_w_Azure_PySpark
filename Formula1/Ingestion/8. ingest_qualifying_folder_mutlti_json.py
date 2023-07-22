# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Qualifying JSON File
# MAGIC
# MAGIC *This is a multi line json, 2 split files, load as one into the data lake*
# MAGIC
# MAGIC **Requirments:**
# MAGIC
# MAGIC 1. Rename qualifyingId, raceId, driverId, construcotrId.
# MAGIC 2. Add ingestion date column.
# MAGIC 3. Write as parquet to clean container.
# MAGIC
# MAGIC **Data overview for schema:** http://ergast.com/docs/f1db_user_guide.txt
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# import types needed to enforce schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# create schema
qualify_schema = StructType(fields=[
    StructField('qualifyId', IntegerType(), False),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('constructorId', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True)
])

# COMMAND ----------

# read in files - apply schema
qualifying_df = spark.read \
    .schema(qualify_schema) \
    .option('multiLine', True) \
    .json(f'{stage_folder_path}/qualifying/qualifying_split*.json')

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# rename the columns - qualifyingId, raceId, driverId, construcotrId using snake case and add ingestion date
qualifying_df_cleaned = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id') \
    .withColumnRenamed('raceId', 'race_id') \
    .withColumnRenamed('driverId', 'driver_id') \
    .withColumnRenamed('constructorId', 'constuctor_id') \
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# view the cleaned results
display(qualifying_df_cleaned)

# COMMAND ----------

# get a count of records - 8,694
qualifying_df_cleaned.count()

# COMMAND ----------

# write to parquet in clean container
qualifying_df_cleaned.write.mode('overwrite').parquet(f'{clean_folder_path}/qualifying')

# COMMAND ----------

# query parquet in clean container
display(spark.read.parquet(f'{clean_folder_path}/qualifying'))

# COMMAND ----------


