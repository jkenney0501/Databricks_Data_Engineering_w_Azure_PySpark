-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### See all databases

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### See the current database and switch to another

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

-- SWITCH TO ANOTHER
USE f1_clean;

-- COMMAND ----------

-- SEE THE TABLES IN THE RAW DB
SHOW TABLES;

-- COMMAND ----------

-- REFRESH TBALE TO CLEAR CACHE IN CASE OF UPDATE
-- REFRESH TABLE drivers;

-- COMMAND ----------

-- see a limimted amount of data from a table
SELECT * 
FROM drivers
LIMIT 3;

-- COMMAND ----------

-- YOU CAN ALOS USE DESCRIBE TO SEE THE ATTRIBUTES OF THE TABLE
DESCRIBE drivers;

-- COMMAND ----------

-- Filter with WHERE and use IN for a list of filter arguements, use AND to additional condition that must be true
SELECT *
FROM drivers 
WHERE nationality IN ('British')
AND dob > "1990-01-01"

-- COMMAND ----------

-- use an alias
SELECT 
name,
dob AS date_of_birth
FROM drivers;

-- COMMAND ----------

-- get the top 3 youngest drivers from britian - order by date of birth with the youngest in descending order
SELECT *
FROM drivers 
WHERE nationality IN ('British')
ORDER BY dob DESC
LIMIT 3;

-- COMMAND ----------


