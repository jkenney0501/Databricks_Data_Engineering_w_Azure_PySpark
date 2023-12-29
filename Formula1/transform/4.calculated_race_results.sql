-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create Presentation Tables for Visualization & Analysis
-- MAGIC - Use HIVE managed tables taht were created in HIVE metastore

-- COMMAND ----------

USE f1_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a table that filters out drivers that finsihed out of the top 10 in all years.
-- MAGIC - Create a custom points calculation since points vary over the years for each position and there would be inconsistentcy.
-- MAGIC - Save this table to the final/presentation layer

-- COMMAND ----------

-- NOTE: this is being created in the final/presentation database
CREATE TABLE f1_final.calculated_race_results
USING PARQUET
SELECT
rcs.race_year,
c.name AS tema_name,
d.name AS driver_name,
res.position,
res.points,
11-res.position AS calculated_points
FROM f1_clean.results AS res
JOIN f1_clean.drivers AS d ON res.driver_id = d.driver_id
JOIN f1_clean.constructors AS c ON res.constructor_id = c.constructor_id
JOIN f1_clean.races AS rcs ON res.race_id = rcs.race_id
WHERE res.position <= 10

-- COMMAND ----------

-- query the table to check results
SELECT COUNT(1)
FROM f1_final.calculated_race_results

-- COMMAND ----------


