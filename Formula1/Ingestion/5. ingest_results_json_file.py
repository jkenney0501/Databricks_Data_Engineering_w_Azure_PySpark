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

# Import required types
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

# create schema 
results_schema = StructType(fields=[
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), False),
    StructField("driverId ", StringType(), False),
    StructField("constructorId ", StringType(), False),
    StructField("number", IntegerType(), True),
    StructField("grid  ", StringType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText ", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("statusId", IntegerType(), True)
])

# COMMAND ----------

# read the json to a dataframe
results_df = spark.read \
            .option("inferschema", True) \
            .json('/mnt/dlformula1jk/stage/results.json')

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

display(results_cleaned)

# COMMAND ----------


