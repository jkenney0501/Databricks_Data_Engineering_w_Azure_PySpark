# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest Constructors JSON File
# MAGIC
# MAGIC **Requirments for constructors file:**
# MAGIC
# MAGIC
# MAGIC 1. Rename construtcorId and constuctorRef to constructor_id and constructor_ref
# MAGIC 2. Drop the url column
# MAGIC 3. Add a new column called ingestion date
# MAGIC 4. Write as a parquet file to the clean container

# COMMAND ----------

# MAGIC %md Find the file in the stage container to read in using file system sematics

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dlformula1jk/stage

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType

# COMMAND ----------

# create schema for constructors file and then read in the json file with the spark json api - /mnt/dlformula1jk/stage/constructors.json
# Using storage explorer to preview the data which helps in writing the schema
constructors_schema = StructType(fields=[
    StructField("constructorId", IntegerType(), False),
    StructField("constructorRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True),
])

# COMMAND ----------

constructors_df = spark.read \
    .format('json') \
    .schema(constructors_schema) \
    .json('/mnt/dlformula1jk/stage/constructors.json')

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Drop the URL column

# COMMAND ----------

constructors_df_clean = constructors_df.drop('url')

constructors_df_clean.show(3)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Add Ingestion Date Column
# MAGIC - Rename construcorId and constuctorRef to constructor_id and constructor_ref 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_df_final = constructors_df_clean.withColumn('ingestion_date', current_timestamp()) \
    .withColumnRenamed('constructorId', 'constructor_id') \
    .withColumnRenamed('constructorRef', 'constructor_ref')

display(constructors_df_final)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Write the file as a parquet to the clean container.

# COMMAND ----------

constructors_df_final.write.mode("overwrite").parquet("/mnt/dlformula1jk/clean/constructors")

# COMMAND ----------

# MAGIC %md Check files for validation

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/dlformula1jk/clean/constructors

# COMMAND ----------


