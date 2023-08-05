-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### This notebook creates a database from raw table storage and creates tables for that raw data.
-- MAGIC - Tables are created in HIVE metastore as EXTERNAL tables (they are stored in ADLS and not the HIVE DWH)
-- MAGIC - Data is ingested similar to how we stage it but using SQL. It will then be a transformed layer and stored in a final/presentation layer.

-- COMMAND ----------

-- use if not exists to avoid exception if it does exist
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- see current database
SELECT current_database();

-- COMMAND ----------

-- change to f1_raw
USE f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create circuits table from circuits.csv file
-- MAGIC - all schema info here: **/mnt/dlformula1jk/stage/circuits.csv**

-- COMMAND ----------

-- DROP TABLE f1_raw.circuits;

-- COMMAND ----------

-- use DDL style 
CREATE TABLE IF NOT EXISTS f1_raw.circuits (
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING CSV
OPTIONS(path "/mnt/dlformula1jk/stage/circuits.csv", header True)

-- COMMAND ----------

-- test query the new table
-- the header is being printed as the first row, this needs adjusted in the table creation clause, add "header True" (no quotes) in the OPTIONS statment. Drop the table first.
-- all fixed
SELECT * 
FROM f1_raw.circuits
LIMIT 3;

-- COMMAND ----------

DESCRIBE EXTENDED f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create races table
-- MAGIC - schema info here: **/mnt/dlformula1jk/stage/races.csv**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races (
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING CSV
OPTIONS(path "/mnt/dlformula1jk/stage/races.csv", header True)

-- COMMAND ----------

-- view the races table
SELECT *
FROM f1_raw.races
LIMIT 3;

-- COMMAND ----------

DESCRIBE EXTENDED f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create consructors table
-- MAGIC - Single line JSON
-- MAGIC - Simple structure (dictionay like KV pairs)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef INT,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS(path "/mnt/dlformula1jk/stage/constructors.json")

-- COMMAND ----------

SELECT *
FROM f1_raw.constructors
LIMIT 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create drivers table
-- MAGIC - Single line JSON
-- MAGIC - Complex structure - name field is nested json

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT <forename: STRING, surname:STRING>,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS(path "/mnt/dlformula1jk/stage/drivers.json")

-- COMMAND ----------

SELECT *
FROM f1_raw.drivers
LIMIT 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create results table
-- MAGIC - Single line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;

CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  driverId INT,
  constructorId INT,
  raceId INT,
  number INT,
  grid INT,
  position INT,
  positionOrder INT,
  positionText INT,
  points DOUBLE,
  laps INT,
  time STRING,
  milliseconds STRING,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed STRING,
  statusId INT
)
USING JSON
OPTIONS(path "/mnt/dlformula1jk/stage/results.json")

-- COMMAND ----------

SELECT *
FROM f1_raw.results
LIMIT 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create pit stops table
-- MAGIC - Multi-line JSON
-- MAGIC - Simple structure
-- MAGIC
-- MAGIC *specify multiLine True in OPTIONS statment*

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  raceId INT,
  driverId INT,
  stop INT,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds STRING
)
USING JSON
OPTIONS(path "/mnt/dlformula1jk/stage/pit_stops.json", multiLine True)

-- COMMAND ----------

SELECT *
FROM f1_raw.pit_stops
LIMIT 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create lap times table
-- MAGIC - CSV file
-- MAGIC - Multiple files: create a table on top of the lap time folder and this will allow all split files to be viewed in a query.

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
    raceId INT,
    driverId INT,
    lap INT,
    position INT,
    time STRING,
    milliseconds STRING
)
USING CSV
OPTIONS(path "/mnt/dlformula1jk/stage/lap_times")

-- COMMAND ----------

SELECT *
FROM f1_raw.lap_times
LIMIT 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create qualifying table
-- MAGIC - Multi-line JSON
-- MAGIC - Multiple files: create a table on top of the qualifying folder

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
    driverId INT,
    constructorId INT,
    number INT,
    position INT,
    q1 STRING,
    q2 STRING,
    q3 STRING,
    qualifyId INT,
    raceId INT
)
USING JSON
OPTIONS(path "/mnt/dlformula1jk/stage/qualifying/qualifying_split*.json", multiLine True)

-- COMMAND ----------

-- get a count
SELECT COUNT(1) FROM f1_raw.qualifying;

-- COMMAND ----------

-- view the results
SELECT *
FROM f1_raw.qualifying
LIMIT 3;

-- COMMAND ----------


