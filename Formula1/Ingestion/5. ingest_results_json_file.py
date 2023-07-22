# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest the Results JSON File
# MAGIC
# MAGIC **Requirements:**
# MAGIC
# MAGIC 1. Rename columns - resultId, raceId, driverId, constructorId, positionText, positionOrder, fastestLap, fastestLapTime, fastestLapSpeed. Use snake case to rename.
# MAGIC 2. Drop statusId.
# MAGIC 3. Add ingestion date column.
# MAGIC 4. Partition by race id.
# MAGIC 5. Write as parquet to clean folder.
# MAGIC
# MAGIC **link to schema info from API:**
# MAGIC
# MAGIC http://ergast.com/docs/f1db_user_guide.txt

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# Import required types
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, LongType

# COMMAND ----------

# create schema 
results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("constructorId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("number",IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("positionText", IntegerType(), True),
    StructField("points", DoubleType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", StringType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), True)
    
])

# COMMAND ----------

# read the json to a dataframe
results_df = spark.read \
            .schema(results_schema) \
            .json(f'{stage_folder_path}/results.json')

# COMMAND ----------

display(results_df)

# COMMAND ----------

# see the schema 
results_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# combine steps 1 and 2 - rename cols and drop status id - resultId, raceId, driverId, constructorId, positionText, positionOrder, fastestLap, fastestLapTime, fastestLapSpeed. Use snake case to rename.
results_cleaned = results_df.withColumnRenamed("resultId", "result_id") \
                            .withColumnRenamed("raceId", "race_id") \
                            .withColumnRenamed("driverId", "driver_id") \
                            .withColumnRenamed("constructorId", "constructor_id") \
                            .withColumnRenamed("positionText", "position_text") \
                            .withColumnRenamed("positionOrder", "position_order") \
                            .withColumnRenamed("fastestLap", "fastest_lap") \
                            .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                            .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                            .withColumn("ingestion_date", current_timestamp()) \
                            .drop("statusId")

# COMMAND ----------

# show cleaned results
display(results_cleaned)

# COMMAND ----------

# parition  by race id and write to clean folder
results_cleaned.write.mode('overwrite').partitionBy('race_id').parquet(f'{clean_folder_path}/results')

# COMMAND ----------

# read in the parquet file using file sysytem semantics
display(spark.read.parquet(f'{clean_folder_path}/results'))

# COMMAND ----------


