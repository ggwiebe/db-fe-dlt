-- Databricks notebook source
-- MAGIC %md ## x. Setup for Database  

-- COMMAND ----------

CREATE WIDGET TEXT root_location DEFAULT "/Users/glenn.wiebe@databricks.com/";
CREATE WIDGET TEXT db_name DEFAULT "ggw_retail";
CREATE WIDGET TEXT data_loc DEFAULT "/data";
-- REMOVE WIDGET old

-- COMMAND ----------

-- DROP DATABASE $db_name;
CREATE DATABASE $db_name
LOCATION "$root_location/$db_name/$db_name.db";

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED $db_name

-- COMMAND ----------

-- MAGIC %md ## y. Setup for CloudFiles  
-- MAGIC   
-- MAGIC e.g. for ggw_retail, use these:
-- MAGIC ```
-- MAGIC %fs mkdirs /Users/glenn.wiebe@databricks.com/ggw_retail
-- MAGIC %fs mkdirs /Users/glenn.wiebe@databricks.com/ggw_retail/data
-- MAGIC %fs mkdirs /Users/glenn.wiebe@databricks.com/ggw_retail/data/in
-- MAGIC ```
-- MAGIC   
-- MAGIC Sample data (insert, append, update & delete) in $db_name/data;  
-- MAGIC Copy the individual files in sequence, to emulate a series of transactions arriving;  
-- MAGIC Copy from $db_name/data to the $db_name/data/in folder for CloudFiles to pickup-up each individual file in order (and keep track of each)

-- COMMAND ----------

-- MAGIC %fs ls /Users/glenn.wiebe@databricks.com/ggw_retail/data/

-- COMMAND ----------

-- Create a "table" definition against all CSV files in the data location
CREATE TABLE $db_name.customers_source 
  (
      id int, first_name string, last_name string, email string, active int, update_dt timestamp, update_user string
  )
 USING CSV
OPTIONS (
    path "$root_location/$db_name/$data_loc/*.csv",
    header "true",
    -- inferSchema "true",
    mode "FAILFAST",
    schema 'id int, first_name string, last_name string, email string, active int, update_dt timestamp, update_user string'
  )
;


-- COMMAND ----------

SELECT *
  FROM $db_name.customers_source
 ORDER BY update_dt, id ASC;

-- COMMAND ----------

-- MAGIC %md ## CREATE/START DLT PIPELINE!!!  
-- MAGIC   
-- MAGIC Once the above infrastructure is in place, start the pipeline (in continuous mode, or start after each of the next steps)

-- COMMAND ----------

-- MAGIC %md ## 1. Copy in first set of records - Insert

-- COMMAND ----------

-- MAGIC %fs cp /Users/glenn.wiebe@databricks.com/ggw_retail/data/customer-1-insert.csv /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/

-- COMMAND ----------

-- Create a "table" definition against all CSV files in the cloudFiles location (e.g. /data/in)
-- This cannot be done until some records are in /data/in
CREATE TABLE $db_name.customers_raw 
  (
      id int, first_name string, last_name string, email string, active int, update_dt timestamp, update_user string
  )
 USING CSV
OPTIONS (
    path "$root_location/$db_name/$data_loc/in/*.csv",
    header "true",
    -- inferSchema "true",
    mode "FAILFAST",
    schema 'id int, first_name string, last_name string, email string, active int, update_dt timestamp, update_user string'
  )
;


-- COMMAND ----------

SELECT * 
  FROM $db_name.customers_raw
 ORDER BY update_dt, id ASC
;  

-- COMMAND ----------

SELECT * 
  FROM $db_name.customer_bronze
 ORDER BY update_dt, id ASC
;  

-- COMMAND ----------

SELECT * 
  FROM $db_name.customer_silver
 ORDER BY update_dt, id ASC
;  

-- COMMAND ----------

-- MAGIC %md ## 2. Copy in second set of records - Append

-- COMMAND ----------

-- MAGIC %fs cp /Users/glenn.wiebe@databricks.com/ggw_retail/data/customer-2-append.csv /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/

-- COMMAND ----------

-- Re-create a "table" definition against all CSV files in the cloudFiles location (e.g. /data/in)
CREATE TABLE $db_name.customers_raw 
  (
      id int, first_name string, last_name string, email string, active int, update_dt timestamp, update_user string
  )
 USING CSV
OPTIONS (
    path "$root_location/$db_name/$data_loc/in/*.csv",
    header "true",
    -- inferSchema "true",
    mode "FAILFAST",
    schema 'id int, first_name string, last_name string, email string, active int, update_dt timestamp, update_user string'
  )
;

-- COMMAND ----------

-- Check Raw
SELECT * 
  FROM ggw_retail.customers_raw
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check Bronze 
SELECT * 
  FROM ggw_retail.customer_bronze
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check Silver 
SELECT * 
  FROM ggw_retail.customer_silver
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- MAGIC %md ## 3. Copy in third set of records - Update

-- COMMAND ----------

-- MAGIC %fs cp /Users/glenn.wiebe@databricks.com/ggw_retail/data/customer-3-update.csv /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/

-- COMMAND ----------

-- Check Raw
SELECT * 
  FROM ggw_retail.customers_raw
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check Bronze 
SELECT * 
  FROM ggw_retail.customer_bronze
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check Silver 
SELECT * 
  FROM ggw_retail.customer_silver
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- MAGIC %md ## 4. Copy in fourth set of records - delete

-- COMMAND ----------

-- MAGIC %fs cp /Users/glenn.wiebe@databricks.com/ggw_retail/data/customer-4-delete.csv /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/

-- COMMAND ----------

-- Check Raw
SELECT * 
  FROM ggw_retail.customers_raw
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check Bronze 
SELECT * 
  FROM ggw_retail.customer_bronze
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- Check Silver 
SELECT * 
  FROM ggw_retail.customer_silver
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- MAGIC %md ## 99. Reset the csv files

-- COMMAND ----------

-- MAGIC %fs rm -r /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/

-- COMMAND ----------

-- MAGIC %fs ls /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/

-- COMMAND ----------

dbutils.notebook.exit()

-- COMMAND ----------

DROP DATABASE $db_name CASCADE;

-- COMMAND ----------

-- MAGIC %md ## Delta Lake based source table

-- COMMAND ----------

-- 0. Create customer table patterned on MySQL schema
-- DROP TABLE ggw_retail.customer;
CREATE TABLE ggw_retail.customer (
  -- id INTEGER NOT NULL GENERATED ALWAYS AS (row_number() OVER(PARTITION BY email ORDER BY first_name)),
  id INTEGER NOT NULL,
  first_name varchar(255) NOT NULL,
  last_name varchar(255) NOT NULL,
  email varchar(255) NOT NULL,
  active INTEGER GENERATED ALWAYS AS (1),
  update_dt timestamp GENERATED ALWAYS AS (now()),
  update_user varchar(128) GENERATED ALWAYS AS (current_user())
);

-- COMMAND ----------

-- 1. insert first records
INSERT INTO ggw_retail.customer
(id, first_name, last_name, email)
VALUES
    (1001, 'Glenn', 'Wiebe', 'ggwiebe@gmail.com'),
    (1002, 'Graeme', 'Wiebe', 'glenn@wiebes.net')
;

-- COMMAND ----------

-- 1.+ Select these first records & the dlt moved records
SELECT *
  FROM ggw_retail.customer
;

-- COMMAND ----------

-- 2. insert more records
INSERT INTO ggw_retail.customer
(id, first_name, last_name, email)
VALUES
    (1003, 'Dillon', 'Bostwick', 'dillon@databricks.com'),
    (1004, 'Franco', 'Patano', 'franco.patano@databricks.com')
;

-- COMMAND ----------

-- 1.+ Select these first records & the dlt moved records
SELECT *
  FROM ggw_retail.customer
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- 3. insert the update records
INSERT INTO ggw_retail.customer
(id, first_name, last_name, email)
VALUES
    (1002, 'Glenn', 'Wiebe', 'glenn@wiebes.net')
;

-- -- 3. update record
-- UPDATE ggw_retail.customer
--    SET first_name = 'Glenn'
--  WHERE id = 1002

-- COMMAND ----------

-- 1.+ Select these first records & the dlt moved records
SELECT *
  FROM ggw_retail.customer
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- 4. insert the Delete record
INSERT INTO ggw_retail.customer
(id, first_name, last_name, email, active)
VALUES
    (1002, 'Glenn', 'Wiebe', 'glenn@wiebes.net', 0)
;

-- -- 4.+ Delete the second Glenn record
-- UPDATE ggw_retail.customer
--  WHERE id = 1002
-- ;
