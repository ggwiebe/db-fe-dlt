-- Databricks notebook source
-- MAGIC %md ## x. Setup for Database  

-- COMMAND ----------

DROP DATABASE ggw_retail;
CREATE DATABASE ggw_retail
LOCATION "dbfs:/Users/glenn.wiebe@databricks.com/ggw_retail/ggw_retail.db";

-- COMMAND ----------

DESCRIBE DATABASE ggw_retail

-- COMMAND ----------

-- MAGIC %md ## y. Setup for CloudFiles  
-- MAGIC   
-- MAGIC ```
-- MAGIC %fs mkdirs /Users/glenn.wiebe@databricks.com/ggw_retail
-- MAGIC %fs mkdirs /Users/glenn.wiebe@databricks.com/ggw_retail/data
-- MAGIC %fs mkdirs /Users/glenn.wiebe@databricks.com/ggw_retail/data/in
-- MAGIC ```
-- MAGIC   
-- MAGIC Sample data (insert, append, update & delete) in ggw_retail/data
-- MAGIC Copy the specific file into ggw_retail/data/in for CloudFiles to pickup-up the data in the files.

-- COMMAND ----------

-- MAGIC %fs mkdirs /Users/glenn.wiebe@databricks.com/ggw_retail/data/in_checkpoint

-- COMMAND ----------

-- MAGIC %fs ls /Users/glenn.wiebe@databricks.com/ggw_retail/data

-- COMMAND ----------

DROP VIEW customers_raw;
CREATE OR REPLACE TEMPORARY VIEW customers_raw 
   (
      id int, first_name string, last_name string, email string, active int, update_dt timestamp, update_user string
   )
 USING CSV
OPTIONS (
    path "/Users/glenn.wiebe@databricks.com/ggw_retail/data/customer-1-insert.csv",
    header "true",
--     inferSchema "true",
--     mode "FAILFAST",
    schema 'id int, first_name string, last_name string, email string, active int, update_dt int, update_user string'
  )
;


-- COMMAND ----------

SELECT *
  FROM customers_raw;

-- COMMAND ----------

-- MAGIC %md ## 1. Copy in first set of records - Insert

-- COMMAND ----------

-- MAGIC %fs cp /Users/glenn.wiebe@databricks.com/ggw_retail/data/customer-1-insert.csv /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = (spark.readStream.format('cloudFiles')
-- MAGIC       .option('cloudFiles.format', 'csv')
-- MAGIC       .option('header', 'true')
-- MAGIC #       .option('inferSchema', 'true')
-- MAGIC       .schema('id int, first_name string, last_name string, email string, active int, update_dt timestamp, update_user string')
-- MAGIC       .load('/Users/glenn.wiebe@databricks.com/ggw_retail/data/in/')
-- MAGIC       )
-- MAGIC 
-- MAGIC df.writeStream.format('delta') \
-- MAGIC   .option('checkpointLocation', '/Users/glenn.wiebe@databricks.com/ggw_retail/data/in_checkpoint') \
-- MAGIC   .start('/Users/glenn.wiebe@databricks.com/ggw_retail/data/delta/')

-- COMMAND ----------

SELECT * 
  FROM ggw_retail.customer_silver
 ORDER BY update_dt, id ASC
;  

-- COMMAND ----------

-- MAGIC %md ## 2. Copy in second set of records - Append

-- COMMAND ----------

-- MAGIC %fs cp /Users/glenn.wiebe@databricks.com/ggw_retail/data/customer-2-append.csv /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/

-- COMMAND ----------

-- SELECT * 
--   FROM ggw_retail.customer_bronze;
SELECT * 
  FROM ggw_retail.customer_silver
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- MAGIC %md ## 3. Copy in third set of records - Update

-- COMMAND ----------

-- MAGIC %fs cp /Users/glenn.wiebe@databricks.com/ggw_retail/data/customer-3-update.csv /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/

-- COMMAND ----------

-- SELECT * 
--   FROM ggw_retail.customer_bronze;
SELECT * 
  FROM ggw_retail.customer_silver
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

-- MAGIC %md ## 4. Copy in four set of records - delete

-- COMMAND ----------

-- MAGIC %fs cp /Users/glenn.wiebe@databricks.com/ggw_retail/data/customer-4-delete.csv /Users/glenn.wiebe@databricks.com/ggw_retail/data/in/

-- COMMAND ----------

SELECT * 
  FROM ggw_retail.customer_bronze
 ORDER BY update_dt, id ASC
;

-- COMMAND ----------

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
