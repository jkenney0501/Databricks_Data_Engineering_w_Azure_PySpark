-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## SQL Joins
-- MAGIC - join tables together by common attribute (typicallly a key in RDBMS)

-- COMMAND ----------

USE DATABASE f1_final;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC driver_standings;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create two views to join them using various join methods.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT
race_year,
driver_name,
team, 
total_points,
wins,
rank
FROM driver_standings
WHERE race_year IN (2018)

-- COMMAND ----------

-- see the view
SELECT *
FROM v_driver_standings_2018
LIMIT 5;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT
race_year,
driver_name,
team, 
total_points,
wins,
rank
FROM driver_standings
WHERE race_year IN (2020)

-- COMMAND ----------

-- see the view
SELECT *
FROM v_driver_standings_2020
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Inner Join**
-- MAGIC - Select records that match from both tables

-- COMMAND ----------

SELECT 
d_2018.*,
d_2020.*
FROM v_driver_standings_2018 AS d_2018
JOIN v_driver_standings_2020 AS d_2020 ON d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC The result of the Inner join produces 15 records because:
-- MAGIC - we joined on driver name, some drove in 2018 and not 2020 and vice versa so we only get records where the driver name appears in both tables.
-- MAGIC
-- MAGIC How to get all records?
-- MAGIC - Take the larger table and make it the left table then left join (some drivers didnt drive in 2018 so a right join would be need, see below). 
-- MAGIC - Where no driver name exists, there will be a null set of values for that row.
-- MAGIC
-- MAGIC **Left Join:**

-- COMMAND ----------

-- nulls will appear in the right table
SELECT 
d_2018.*,
d_2020.*
FROM v_driver_standings_2018 AS d_2018
LEFT JOIN v_driver_standings_2020 AS d_2020 ON d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Right Join**
-- MAGIC - shows all the table values on the right table. Some drivers didnt drive in 2018 so they are new.

-- COMMAND ----------

-- now there are 24 rows, the nulls will now appear in the left table rows where the driver name is null
SELECT 
d_2018.*,
d_2020.*
FROM v_driver_standings_2018 AS d_2018
RIGHT JOIN v_driver_standings_2020 AS d_2020 ON d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Full Outer Join**
-- MAGIC - gets all records from both tables
-- MAGIC - where records dont match or exist, there will be nulls on both left and right tables

-- COMMAND ----------

SELECT 
d_2018.*,
d_2020.*
FROM v_driver_standings_2018 AS d_2018
FULL JOIN v_driver_standings_2020 AS d_2020 ON d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Anti Join**
-- MAGIC - produces records that show what records dont exist on the right table whne we left join, for example-the above left join shows 5 nulls on the right table, this output would represent thos 5 drivers that didint drive in 2020.

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 AS d_2018
ANTI JOIN v_driver_standings_2020 AS d_2020 ON d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Cross Join**
-- MAGIC - Not ever wroth using....
-- MAGIC - producces a cartesian product where it shows all possible combinations - 20 * 24 = 480
-- MAGIC - This often happens as an error when the join isnt qualified correctly or in error

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 AS d_2018
CROSS JOIN v_driver_standings_2020 AS d_2020 

-- COMMAND ----------


