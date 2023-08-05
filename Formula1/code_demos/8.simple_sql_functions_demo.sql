-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Simple SQL Functions
-- MAGIC - There are many built in functions, see link below:
-- MAGIC
-- MAGIC Link: https://spark.apache.org/docs/latest/api/sql/index.html

-- COMMAND ----------

USE f1_clean;

-- COMMAND ----------

DESC drivers;

-- COMMAND ----------

-- CONCAT function to combine to strings with a delimiter in between (doesnt require a delim)
-- use driver table for examples
SELECT *, 
CONCAT(driver_ref, '-', code) AS new_driver_ref
FROM drivers;

-- COMMAND ----------

-- SPLIT name into forename and surname
SELECT *,
SPLIT(name, " ")[0] AS forename,
SPLIT(name, " ")[1] AS surname
FROM drivers;

-- COMMAND ----------

-- current date
SELECT *, 
current_date() AS current_dt
FROM drivers;

-- COMMAND ----------

--change the date format for dob
SELECT *,
date_format(dob, 'dd-MM-yyyy') as new_date
FROM drivers;

-- COMMAND ----------

-- get THE MAXIMUM value for driver id
SELECT 
MAX(driver_id) as high_id
FROM drivers;

-- COMMAND ----------

-- get the min
SELECT 
MIN(driver_id) as high_id
FROM drivers;

-- COMMAND ----------

-- CONVERT TO UPPER
SELECT *,
UPPER(driver_ref) AS CAPS_REF
FROM drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Window and aggregate functions

-- COMMAND ----------

-- get a count
SELECT COUNT(1)
FROM drivers
WHERE nationality IN ('British');

-- COMMAND ----------

-- get counts by nationality for all with group by and order from the most to least
SELECT 
nationality,
COUNT(*) AS total
FROM drivers
GROUP BY nationality
ORDER BY total DESC;

-- COMMAND ----------

-- use HAVING to get countries with more than 100 drivers
SELECT 
nationality,
COUNT(*) AS total
FROM drivers
GROUP BY nationality
HAVING total >= 100
ORDER BY total DESC;

-- COMMAND ----------

-- rank drivers by nationality with RANK
SELECT 
nationality,
name,
dob,
RANK() OVER(PARTITION BY nationality ORDER BY dob DESC ) AS driver_rnk_nationality
FROM drivers
WHERE dob >= '1970-01-01';

-- COMMAND ----------

-- add date functions later
