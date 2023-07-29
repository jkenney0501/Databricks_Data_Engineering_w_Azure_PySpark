-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Objectives
-- MAGIC 1. Spark SQL documentation - https://spark.apache.org/docs/latest/sql-ref.html
-- MAGIC 2. Create database demo
-- MAGIC 3. Data tab in UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find current database
-- MAGIC 7. CREATE MANAGED table
-- MAGIC 8. CREATE EXTERNAL table

-- COMMAND ----------

-- create demo db, SQL is selected as primary language in databricks so no %sql needed
CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

-- view the databases
SHOW DATABASES;

-- COMMAND ----------

-- select db to use
USE demo;

-- COMMAND ----------

-- describe the demo database, this gives you the info such as catalog info, location, namespace and owner
-- location for sql db' are the hive warehouse, for example this one is dbfs:/user/hive/warehouse/demo.db
DESCRIBE DATABASE demo;

-- COMMAND ----------

-- check what db you are using
SELECT current_database();

-- COMMAND ----------

-- see the tables in your db, no tables yet
SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managed Tables
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configurations"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # managed table in python - using race results in final container and saving as a table
-- MAGIC # this is managed becaseuse we are using the hive metastore and ADLS, if we delete the data here, the metatable is also delted with the data and vice versa.
-- MAGIC race_results_df = spark.read.parquet(f'{final_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # write the df to a table in the demo database
-- MAGIC race_results_df \
-- MAGIC     .write \
-- MAGIC     .format("parquet") \
-- MAGIC     .saveAsTable("demo.race_results_python") # this creates a managed table

-- COMMAND ----------

-- query the results with sql in the demo db
SELECT *
FROM race_results_python
LIMIT 5;

-- COMMAND ----------

-- describe the table with details
DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

-- Create managed table using SQL
CREATE TABLE IF NOT EXISTS demo.race_results_sql
AS
SELECT *
FROM demo.race_results_python
WHERE race_year IN (2020);

-- COMMAND ----------

-- query the new managed table
SELECT * 
FROM demo.race_results_sql;

-- COMMAND ----------

-- Dropping either table will result in the info being removed from the hive metstore and our database becasue it is a managed table. Both data and metadata are dropped.
DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## External Tables
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping external table - only metadata is removed, data stays where its at.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # write the df to a table in the demo database
-- MAGIC # primary difference is just adding the path, hive does not manage the storage in an external table
-- MAGIC race_results_df \
-- MAGIC     .write \
-- MAGIC     .format("parquet") \
-- MAGIC     .option("path", f"{final_folder_path}/race_results_ext") \
-- MAGIC     .saveAsTable("demo.race_results_ext") 

-- COMMAND ----------

-- TYPE WILL = EXTERNAL
DESCRIBE EXTENDED demo.race_results_ext;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create external table with SQL
-- MAGIC - Using the traditionl INSERT method

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_ext_sql
(
race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points DOUBLE,
position INT,
created_at TIMESTAMP
)
USING PARQUET
LOCATION "/mnt/dlformula1jk/final/race_results_ext_sql"

-- COMMAND ----------

-- insert the data into the table
INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext WHERE race_year = 2020;

-- COMMAND ----------

-- data has been inserted!
SELECT *
FROM demo.race_results_ext_sql
LIMIT 3;

-- COMMAND ----------

-- drop the external table, only the metadata will be dropped from HIve, the folder still exists in ADLS
DROP TABLE demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Views on tables
-- MAGIC 1. Create a temp view
-- MAGIC 2. Create a global temp view
-- MAGIC 3. Create a permanent view
-- MAGIC
-- MAGIC A view simply queries the data from the location and returns the results. It doesnt store data, they are essentially a read only tool used for business users to query data in production.

-- COMMAND ----------

-- create temp view
CREATE OR REPLACE TEMPORARY VIEW v_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year > 2018;

-- COMMAND ----------

-- query the view
SELECT *
FROM v_race_results;

-- COMMAND ----------

-- Creat a global temp view
CREATE OR REPLACE GLOBAL TEMPORARY VIEW gv_race_results_17_18
AS
SELECT *
FROM demo.race_results_python
WHERE race_year BETWEEN 2017 AND 2017;

-- COMMAND ----------

-- query the view
SELECT *
FROM global_temp.gv_race_results_17_18;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create permanent view
-- MAGIC - registered on Hive metastore

-- COMMAND ----------

CREATE OR REPLACE VIEW vp_race_results_14
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2014;

-- COMMAND ----------

SELECT *
FROM vp_race_results_14;

-- COMMAND ----------

-- see the registed view in the metastore
SHOW TABLES;

-- COMMAND ----------

-- to drop a view:
DROP VIEW v_race_results;

-- COMMAND ----------


