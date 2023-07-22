# Databricks notebook source
# add ingestion date function - take an input data frame, add a column called ingestion date using current timestamp function and out a new df with the new column
# NOTE: this function is only used once becasue ingestion_date was added using method chaining in nearly every notebook which would've caused ALOT of refactoring and a two step process (all renames, add cols are currently completed in one). In the future, I would use the function!
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date', current_timestamp())
    return output_df

# COMMAND ----------


