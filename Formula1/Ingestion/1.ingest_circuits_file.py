# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Circuits.csv File

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1**
# MAGIC 1. Read in with spark dataframe reader

# COMMAND ----------

# get the path for the file first
display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dlformula1jk/stage

# COMMAND ----------

# import data types needed for schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# Specify the schema
circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# read in the df using the path-file system semantics given we have set up service principal with storage mounted
circuits_df = spark.read \
    .option('header', True) \
    .schema(circuits_schema) \
    .csv('dbfs:/mnt/dlformula1jk/stage/circuits.csv')

# COMMAND ----------

# check the schema to make sure data types are enforced correctly - null is not being enforced. This has to be handled expliclty with other functions.
circuits_df.printSchema()

# COMMAND ----------

# check the df using display for a cleaner look at the df
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selecting the Required Columns
# MAGIC - Method 1 just lets you select the columns while the other let you apply column based functions like alias.
# MAGIC
# MAGIC **Step 2** \
# MAGIC 2. Select Columns in several ways

# COMMAND ----------

# example of using select to get the columns
circuits_select_df1 = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

circuits_select_df.show(3)

# COMMAND ----------

# version 2 of above with the dataframe nameusing dot notation
circuits_select_df2 = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

circuits_select_df2.show(3)

# COMMAND ----------

# method 3
circuits_select_df3 = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

circuits_select_df3.show(3)

# COMMAND ----------

# Step 2. method 4 - Im using this one
from pyspark.sql.functions import col

# COMMAND ----------

circuits_select_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country").alias("race_country"), col("lat"), col("lng"), col("alt"))

circuits_select_df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming Columns
# MAGIC - Use snake case as is a standard python convention
# MAGIC
# MAGIC **Step 3**

# COMMAND ----------

# Step 3.
circuits_renamed_df = circuits_select_df.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "lattitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude") 

# COMMAND ----------

# see the renamed df
display(circuits_renamed_df)

# COMMAND ----------


