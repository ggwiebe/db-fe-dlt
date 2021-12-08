-- Databricks notebook source
-- DBTITLE 0,Evolution of the data landscape
-- MAGIC %md # Evolution of Data Landscape
-- MAGIC <img src=https://databricks.com/wp-content/uploads/2020/01/data-lakehouse.png width=1200px>

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # SETUP
-- MAGIC 
-- MAGIC dbutils.widgets.text('database')
-- MAGIC dbutils.widgets.text('path')
-- MAGIC 
-- MAGIC 
-- MAGIC DB = dbutils.widgets.get('database')
-- MAGIC PATH = dbutils.widgets.get('path')
-- MAGIC 
-- MAGIC dbutils.fs.rm('{}/deltademo'.format(PATH), True)
-- MAGIC dbutils.fs.mkdirs('{}/deltademo'.format(PATH))
-- MAGIC 
-- MAGIC dbutils.fs.cp('/databricks-datasets/amazon', '{}/deltademo/amazon'.format(PATH), True)
-- MAGIC dbutils.fs.cp('/databricks-datasets/iot', '{}/deltademo/iot'.format(PATH), True)
-- MAGIC 
-- MAGIC sql('DROP DATABASE IF EXISTS {} CASCADE'.format(DB))
-- MAGIC sql('CREATE DATABASE {}'.format(DB))
-- MAGIC sql('USE {}'.format(DB))
-- MAGIC 
-- MAGIC CP_PATH = "{}/checkpoints".format(PATH)
-- MAGIC 
-- MAGIC spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

-- COMMAND ----------

-- MAGIC %md # Part 1: Ingest
-- MAGIC <img src=https://databricks.com/wp-content/uploads/2021/08/delta-data-ingestion.jpg width=1200px/>

-- COMMAND ----------

-- MAGIC %md ## Stream Ingest

-- COMMAND ----------

-- DBTITLE 1,Clear Stream Checkpoint Folder
-- MAGIC %fs rm -r /Users/glenn.wiebe@databricks.com/dlt_demo/checkpoints

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Set necessary parms
-- MAGIC input_path = "/Users/glenn.wiebe@databricks.com/dlt_demo/landing"
-- MAGIC infer_schema_path = "/Users/glenn.wiebe@databricks.com/dlt_demo/landing"
-- MAGIC 
-- MAGIC # Read Separate from write
-- MAGIC df = (spark.readStream.format("cloudFiles") \
-- MAGIC   .option("cloudFiles.format", "json") \
-- MAGIC   .option("cloudFiles.schemaLocation", infer_schema_path)
-- MAGIC   .option("inferSchema", "true") \
-- MAGIC   .load(input_path)
-- MAGIC )
-- MAGIC 
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %fs ls /Users/glenn.wiebe@databricks.com/dlt_demo/landing/

-- COMMAND ----------

-- DBTITLE 1,Read Stream, Write Delta & Create Table
-- MAGIC %python
-- MAGIC # Set necessary parms
-- MAGIC input_path        = "/Users/glenn.wiebe@databricks.com/dlt_demo/landing"
-- MAGIC infer_schema_path = "/Users/glenn.wiebe@databricks.com/dlt_demo/landing"
-- MAGIC checkpoint_path   = "/Users/glenn.wiebe@databricks.com/dlt_demo/checkpoints"
-- MAGIC delta_path        = "/Users/glenn.wiebe@databricks.com/dlt_demo/delta"
-- MAGIC table_name_unmngd = "ggw_loans.loan_stream_unmngd"
-- MAGIC 
-- MAGIC # Read Separate from write
-- MAGIC df = (spark.readStream.format("cloudFiles") 
-- MAGIC   .option("cloudFiles.format", "json") 
-- MAGIC   .option("cloudFiles.schemaLocation", infer_schema_path)
-- MAGIC   .option("inferSchema", "true") 
-- MAGIC   .queryName("Loan_From_Stream")
-- MAGIC   .load(input_path)
-- MAGIC )
-- MAGIC 
-- MAGIC # Write option 1 - write to unmanaged delta
-- MAGIC (df.writeStream.format("delta")
-- MAGIC   .option("checkpointLocation", checkpoint_path)
-- MAGIC   .trigger(once=True)
-- MAGIC   .start(delta_path)
-- MAGIC )
-- MAGIC 
-- MAGIC # print("CREATE TABLE {} USING DELTA LOCATION '{}'".format(table_name_unmngd,delta_path))
-- MAGIC # spark.sql("DROP TABLE {}".format(table_name_unmngd))
-- MAGIC spark.sql("CREATE TABLE {} USING DELTA LOCATION '{}'".format(table_name_unmngd,delta_path))

-- COMMAND ----------

-- MAGIC %fs ls /Users/glenn.wiebe@databricks.com/dlt_demo/delta

-- COMMAND ----------

SELECT *
  FROM ggw_loans.loan_stream_unmngd
 LIMIT 10
;

-- COMMAND ----------

-- DBTITLE 1,Clear Stream Checkpoint Folder
-- MAGIC %fs rm -r /Users/glenn.wiebe@databricks.com/dlt_demo/checkpoints

-- COMMAND ----------

-- DBTITLE 1,Read Stream, Write Delta Table
-- MAGIC %python
-- MAGIC # Set necessary parms
-- MAGIC input_path = "/Users/glenn.wiebe@databricks.com/dlt_demo/landing"
-- MAGIC infer_schema_path = "/Users/glenn.wiebe@databricks.com/dlt_demo/landing"
-- MAGIC checkpoint_path = "/Users/glenn.wiebe@databricks.com/dlt_demo/checkpoints"
-- MAGIC table_name = "ggw_loans.loan_stream_mngd"
-- MAGIC 
-- MAGIC # Read Separate from write
-- MAGIC df = (spark.readStream.format("cloudFiles") \
-- MAGIC   .option("cloudFiles.format", "json") \
-- MAGIC   .option("cloudFiles.schemaLocation", infer_schema_path)
-- MAGIC   .option("inferSchema", "true") \
-- MAGIC   .load(input_path)
-- MAGIC )
-- MAGIC 
-- MAGIC # Write option 2 - write to managed delta
-- MAGIC ret = (df.writeStream.format("delta") 
-- MAGIC   .option("checkpointLocation", checkpoint_path) 
-- MAGIC   .queryName("Loan_From_Stream")
-- MAGIC   .trigger(once=True)
-- MAGIC   .table(table_name)
-- MAGIC )
-- MAGIC 
-- MAGIC ret_df = spark.sql("SELECT COUNT(*) RowCount FROM {}".format(table_name))
-- MAGIC row_count = ret_df.first()['RowCount']
-- MAGIC print("Dataframe df.count() = {}, writeStream return = {}".format(row_count,ret))

-- COMMAND ----------

SELECT * 
  FROM ggw_loans.loan_stream_mngd
 LIMIT 10
;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # input_path = "/Users/glenn.wiebe@databricks.com/dlt_demo/landing"
-- MAGIC # infer_schema_path = "/Users/glenn.wiebe@databricks.com/dlt_demo/landing"
-- MAGIC #delta_path = "/Users/glenn.wiebe@databricks.com/dlt_demo/delta"
-- MAGIC 
-- MAGIC # Read & write in one step
-- MAGIC ret = (spark.readStream.format("cloudFiles") 
-- MAGIC   .option("cloudFiles.format", "json") 
-- MAGIC   .option("cloudFiles.schemaLocation", infer_schema_path)
-- MAGIC   .option("inferSchema", "true") 
-- MAGIC   .load(input_path) 
-- MAGIC   .writeStream 
-- MAGIC   .option("mergeSchema", "true") 
-- MAGIC   .start(delta_path)

-- COMMAND ----------

-- MAGIC %md ## Parquet to Delta

-- COMMAND ----------

-- DBTITLE 1,Example #1: Convert Parquet data on S3
CONVERT TO DELTA parquet.`/home/dillon.bostwick@databricks.com/deltademo/amazon/data20K`;

SELECT * FROM delta.`/home/dillon.bostwick@databricks.com/deltademo/amazon/data20K`

-- COMMAND ----------

SELECT * FROM delta.`/home/dillon.bostwick@databricks.com/deltademo/amazon/data20K`

-- COMMAND ----------

-- MAGIC %fs ls /home/dillon.bostwick@databricks.com/deltademo/amazon/data20K

-- COMMAND ----------

-- MAGIC %md ## JSON Files

-- COMMAND ----------

-- MAGIC %fs ls /databricks-datasets/iot

-- COMMAND ----------

-- DBTITLE 0,Example #2: JSON files
CREATE TABLE ggw_loans.IOTEventsBronze
USING delta
AS SELECT * FROM json.`dbfs:/databricks-datasets/iot/iot_devices.json`;

SELECT * FROM ggw_loans.IOTEventsBronze;

-- COMMAND ----------

-- DBTITLE 0,Other options: Use a Partner ETL service
-- MAGIC %md ## Partner ETL
-- MAGIC 
-- MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/partner-integrations-and-sources.png" width="750" height="750">

-- COMMAND ----------

-- MAGIC %md # Part 2: Unified Data Lake
-- MAGIC ![delta1](files/dillon/delta1.png)

-- COMMAND ----------

-- DBTITLE 1,Example #1: Upsert into Silver table
CREATE TABLE IOTDevicesSilver (device_name STRING, latest_CO2 INT)
USING Delta;

MERGE INTO IOTDevicesSilver
USING IOTEventsBronze
ON IOTEventsBronze.device_name = IOTDevicesSilver.device_name
WHEN MATCHED
  THEN UPDATE SET latest_CO2 = c02_level
WHEN NOT MATCHED
  THEN INSERT (device_name, latest_CO2) VALUES (device_name, c02_level);

SELECT * FROM IOTDevicesSilver;

-- COMMAND ----------

-- DBTITLE 1,Example #2: Partitioning, Indexing, Caching
OPTIMIZE IOTDevicesSilver ZORDER BY device_name

-- COMMAND ----------

-- DBTITLE 1,Example #3: Consistent batch reads while streaming/writing
-- MAGIC %python
-- MAGIC spark.readStream.format('delta').table('IOTEventsBronze') \
-- MAGIC   .writeStream.format('delta') \
-- MAGIC   .option('checkpointLocation', '{}/IOTEvents'.format(CP_PATH)) \
-- MAGIC   .table('IOTEventsSilverRealtime')

-- COMMAND ----------

SELECT * FROM IOTDevicesSilver VERSION AS OF 0

-- COMMAND ----------

-- DBTITLE 1,Example #4: Replay historical data
DESCRIBE HISTORY IOTDevicesSilver

-- COMMAND ----------

-- DBTITLE 1,Vacuuming and GDPR
 VACUUM IOTEventsBronze RETAIN 0 HOURS

-- COMMAND ----------

SELECT * FROM IOTEventsSilverRealtime

-- COMMAND ----------

-- MAGIC %md # Part 3: Serve Analytics
-- MAGIC ![delta2](files/dillon/delta2.png)

-- COMMAND ----------

-- DBTITLE 1,Example #1: Build aggregates, Gold tables directly in Delta
CREATE TABLE IOTRollupGold
USING delta
AS SELECT
  device_name,
  c02_level,
  battery_level
FROM
  IOTEventsBronze
GROUP BY
  ROLLUP (device_name, c02_level, battery_level)
ORDER BY
  battery_level ASC,
  C02_level DESC;

-- COMMAND ----------

OPTIMIZE IOTRollupGold ZORDER BY battery_level

-- COMMAND ----------

-- DBTITLE 1,Example #2: Query Directly with SparkSQL
SELECT count(*) as Total_Devices FROM IOTRollupGold
WHERE battery_level = 0

-- COMMAND ----------

SELECT * FROM IOTRollupGold

-- COMMAND ----------

-- DBTITLE 1,Example #3: Serve BI from Gold table
-- MAGIC %md
-- MAGIC 
-- MAGIC <img src="/files/dillon/delta-externals.png" width=1100 height=1100>
-- MAGIC 
-- MAGIC ---
-- MAGIC 
-- MAGIC [BI Documentation](https://docs.databricks.com/integrations/bi/index.html);
-- MAGIC [External Reader Documentation](https://docs.databricks.com/delta/integrations.html)

-- COMMAND ----------

-- DBTITLE 1,Example #4: Machine Learning
-- MAGIC %python
-- MAGIC 
-- MAGIC import nltk
-- MAGIC from nltk.sentiment.vader import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC from pyspark.sql.functions import col
-- MAGIC 
-- MAGIC def sentimentNLTK(text):
-- MAGIC   nltk.download('vader_lexicon')
-- MAGIC   return SentimentIntensityAnalyzer().polarity_scores(text)
-- MAGIC 
-- MAGIC spark.udf.register('analyzeSentiment', sentimentNLTK, MapType(StringType(), DoubleType()))

-- COMMAND ----------

SELECT *, analyzeSentiment(review) AS sentiment
FROM delta.`/home/dillon.bostwick@databricks.com/deltademo/amazon/data20K`
ORDER BY sentiment.neg DESC

-- COMMAND ----------

-- MAGIC %md # Extras

-- COMMAND ----------

-- DBTITLE 1,Delta Implementation
-- MAGIC %fs ls /home/dillon.bostwick@databricks.com/deltademo/amazon/data20K

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/home/dillon.bostwick@databricks.com/deltademo/amazon/data20K/_delta_log/

-- COMMAND ----------

-- DBTITLE 1,Backwards Compatibility
VACUUM IOTEventsBronze RETAIN 24*90 hours

-- COMMAND ----------


