-- Databricks notebook source
-- MAGIC %md # Setup Database & File System for Pipeline
-- MAGIC   

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("db_name", "ggw_ods")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC db_name = dbutils.widgets.get("db_name")
-- MAGIC print("Using database name: {}".format(db_name))

-- COMMAND ----------

-- MAGIC %md ## Create DB
-- MAGIC   
-- MAGIC Use ADLS Location

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS $db_name
       LOCATION 'abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/${db_name}/${db_name}.db'

-- COMMAND ----------

use ggw_ods;

-- COMMAND ----------

-- MAGIC %md ## Check tables  
-- MAGIC   
-- MAGIC After Ingest2Table, we should see a table in the list 

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md ## Create File System Structure

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs('abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/{}/data/in'.format(db_name))
-- MAGIC dbutils.fs.mkdirs('abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/{}/data/out'.format(db_name))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/{}'.format(db_name))
