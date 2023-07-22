# Databricks notebook source
# MAGIC %md 
# MAGIC ## Ingest the races csv file:
# MAGIC 1. Drop the url column
# MAGIC 2. Transform-Rename raceId, year and circuitId to use snake case.
# MAGIC 3. Transform-create a new column to concatenate date and time to create race_timestamp.
# MAGIC 4. Add new column called ingestion_date using current timestamp.
# MAGIC 5. Write file as parquet to sink.

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

# MAGIC %md See the files in the container

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dlformula1jk/stage

# COMMAND ----------

# MAGIC %md
# MAGIC **Create the schema**

# COMMAND ----------

# Import the required packages
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[
    StructField('raceId', IntegerType(), False),
    StructField('year', IntegerType(), True),
    StructField('round', IntegerType(), True),
    StructField('circuitId', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('date', DateType(), True),
    StructField('time', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

# import the races.csv file. Infer the schema for now.
races_df_stg = spark.read \
    .option('header', True) \
    .schema(races_schema) \
    .csv(f'{stage_folder_path}/races.csv')

# COMMAND ----------

# see the schema 
races_df_stg.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1** 
# MAGIC - Drop the url column

# COMMAND ----------

# drop the url column since its not needed
races_df_stg = races_df_stg.drop('url')

display(races_df_stg)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2**
# MAGIC - Transform-Rename raceId, year and circuitId to use snake case.

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_df_renamed = races_df_stg.withColumnRenamed('raceid', 'race_id') \
                               .withColumnRenamed('year', 'race_year') \
                               .withColumnRenamed('circuitId', 'circuit_id') \
                               .withColumn("data_source", lit(v_data_source))

races_df_renamed.show(3)                               

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3 & 4**
# MAGIC - Transform-create a new column to concatenate date and time to create race_timestamp.
# MAGIC - Add new column called ingestion_date using current timestamp.

# COMMAND ----------

from pyspark.sql.functions import concat, lit, col, to_timestamp, current_timestamp

# COMMAND ----------

# the format for timestamp has a space between the date and time, lit should also be ' ' otherwise null values will show up.
races_df_transformed = races_df_renamed.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                       .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

races_df_transformed.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 5**
# MAGIC - Write the data as a parquet and store it in the clean container.

# COMMAND ----------

# add a partition for race year
races_df_transformed.write.mode('overwrite').partitionBy('race_year').parquet(f'{clean_folder_path}/races')

# COMMAND ----------

# MAGIC %md see the files in the container

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/dlformula1jk/clean/races

# COMMAND ----------

# read the parquet file
display(spark.read.parquet(f'{clean_folder_path}/races'))

# COMMAND ----------

dbutils.notebook.exit("Workflow was successful")
