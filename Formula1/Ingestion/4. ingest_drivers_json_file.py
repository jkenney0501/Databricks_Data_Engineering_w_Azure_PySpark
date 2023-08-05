# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Drivers JSON File
# MAGIC
# MAGIC **Requirements:**
# MAGIC
# MAGIC 1. Drop url column.
# MAGIC 2. Rename driverId and driverRef to driver_id, driver_ref.
# MAGIC 3. Combine forname and surname as a new column called name (these are nested fields we are concatentating).
# MAGIC 4. Add ingestion date as a new column.
# MAGIC 5. Write as parquet to clean container.

# COMMAND ----------

# add widget for data source to be captured in a column
dbutils.widgets.text("p_data_source", "")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC find file path for file 

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/dlformula1jk/stage

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

# given the schema is nested for name, we have to define in two separate schemas. Start with name(s) 
name_schema = StructType(fields=[
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

# COMMAND ----------

# create the schema (use storage explorer to see the fields) pth = /mnt/dlformula1jk/stage/drivers.json
driver_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# read in the file
driver_df = spark.read \
            .schema(driver_schema) \
            .json(f'{stage_folder_path}/drivers.json')

# COMMAND ----------

# see the schema
driver_df.printSchema()

# COMMAND ----------

display(driver_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# drop the url and rename driverId and driverRef to driver_id, driver_ref. Note steps 1,2,3 can be combined but for practical purposes....
driver_df_cleaned = driver_df.drop('url') \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("data_source", lit(v_data_source))

display(driver_df_cleaned)

# COMMAND ----------

# import needed function to finish up
from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

# deal with the stupid nested name and add ingestion date column
driver_df_final = driver_df_cleaned.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
    .drop("name.forename") \
    .drop("name.surname")

display(driver_df_final)

# COMMAND ----------

# write the output to a parquet file - drivers folder in the clean container
driver_df_final.write.mode("overwrite").format('parquet').saveAsTable('f1_clean.drivers')

# COMMAND ----------

# display the parquert file
display(spark.read.parquet(f"{clean_folder_path}/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Workflow was successful")
