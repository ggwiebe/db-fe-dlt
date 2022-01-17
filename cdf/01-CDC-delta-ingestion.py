# Databricks notebook source
#SKIP_ON_DBC_ARCHIVE
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Implement CDC: Change Data Capture
# MAGIC ## Use-case: Synchronize your SQL Database with your Lakehouse
# MAGIC 
# MAGIC Delta Lake is an <a href="https://delta.io/" target="_blank">open-source</a> storage layer with Transactional capabilities and increased Performances. 
# MAGIC 
# MAGIC Delta lake is designed to support CDC workload by providing support for UPDATE / DELETE and MERGE operation.
# MAGIC 
# MAGIC In addition, Delta table can support CDC to capture internal changes and propagate the changes downstream.
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Fretail%2Fadvanced_de%2Fcdc%2Fcdc_01&dt=DE">
# MAGIC <!-- [metadata={"description":"Process CDC from external system and save them as a Delta Table. BRONZE/SILVER.<br/><i>Usage: demo CDC flow.</i>",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["autoloader", "copy into", "cdc", "cdf"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

# MAGIC %fs mounts

# COMMAND ----------

# dbutils.fs.unmount("/mnt/field-demos")

# COMMAND ----------

# MAGIC %fs ls /mnt/field-demos

# COMMAND ----------

# MAGIC %run ../../demo-retail/_resources/00-setup $reset_all_data=$reset_all_data

# COMMAND ----------

display_slide('1cdpi5arOlmtS80qH45uo-G9NWuHU7KXIxYy6xfqMXWg', '9') #hide this code

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC flow
# MAGIC 
# MAGIC Here is the flow we'll implement, consuming CDC data from an external database. Note that the incoming could be any format, including message queue such as Kafka.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail-cdc-forecast/resources/images/cdc-flow-0.png" alt='Make all your data ready for BI and ML'/>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Incremental data loading using Auto Loader
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail-cdc-forecast/resources/images/cdc-flow-1.png" alt='Make all your data ready for BI and ML' style='float: right' width='600'/>
# MAGIC 
# MAGIC Working with external system can be challenging due to schema update. The external database can have schema update, adding or modifying columns, and our system must be robust against these changes.
# MAGIC 
# MAGIC Databricks Autoloader (`cloudFiles`) handles schema inference and evolution out of the box

# COMMAND ----------

# DBTITLE 1,Let's explore our incoming data. We receive CSV files with client information
# MAGIC %sql 
# MAGIC select * from csv.`/mnt/field-demos/field-demos/retail/raw_cdc`

# COMMAND ----------

# DBTITLE 1,We need to keep the cdc, however csv isn't a efficient storage. Let's put that in a Delta table instead:
bronzeDF = spark.readStream \
                .format("cloudFiles") \
                .option("cloudFiles.format", "csv") \
                .option("cloudFiles.inferColumnTypes", "true") \
                .schema("name string, address string, email string, id long, operation string, operation_date timestamp") \
                .load("/mnt/field-demos/field-demos/retail/raw_cdc") 
                  
bronzeDF.withColumn("file_name", input_file_name()).writeStream \
        .option("checkpointLocation", cloud_storage_path+"/checkpoint_cdc_raw") \
        .trigger(processingTime='10 seconds') \
        .table("clients_cdc")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM clients_cdc order by id asc ;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Materializing the silver table
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail-cdc-forecast/resources/images/cdc-flow-2.png" alt='Make all your data ready for BI and ML' style='float: right' width='600'/>
# MAGIC 
# MAGIC The silver `retail_client_silver` table will contains the most up to date view. It'll be a replicat of the original MYSQL table.
# MAGIC 
# MAGIC Because we'll propagate the `MERGE` operations downstream to the `GOLD` layer, we need to enable Delta Lake CDF: `delta.enableChangeDataCapture = true`

# COMMAND ----------

# DBTITLE 1,We can now create our client table using standard SQL command
# MAGIC %sql 
# MAGIC -- we can add NOT NULL in our ID field (or even more advanced constraint)
# MAGIC CREATE TABLE IF NOT EXISTS retail_client_silver (id BIGINT NOT NULL, name STRING, address STRING, email STRING, operation_date TIMESTAMP) USING delta TBLPROPERTIES (delta.enableChangeDataCapture = true);

# COMMAND ----------

# DBTITLE 1,And run our MERGE statement the upsert the CDC information in our final table
#for each batch / incremental update from the raw cdc table, we'll run a MERGE on the silver table
def merge_stream(df, i):
  df.createOrReplaceTempView("clients_cdc_microbatch")
  #First we need to dedup the incoming data based on ID (we can have multiple update of the same row in our incoming data)
  #Then we run the merge (upsert or delete). We could do it with a window and filter on rank() == 1 too
  df._jdf.sparkSession().sql("""MERGE INTO retail_client_silver target
                                USING
                                (select id, name, address, email, operation, operation_date from 
                                  (SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY operation_date DESC) as rank from clients_cdc_microbatch) 
                                 where rank = 1
                                ) as source
                                ON source.id = target.id
                                WHEN MATCHED AND source.operation = 'DELETE' THEN DELETE
                                WHEN MATCHED AND source.operation != 'DELETE' THEN UPDATE SET *
                                WHEN NOT MATCHED AND source.operation != 'DELETE' THEN INSERT *""")
  
spark.readStream \
       .table("clients_cdc") \
     .writeStream \
       .foreachBatch(merge_stream) \
       .option("checkpointLocation", cloud_storage_path+"/checkpoint_clients_cdc") \
       .trigger(processingTime='10 seconds') \
     .start()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from retail_client_silver order by id asc;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing the first CDC layer
# MAGIC Let's send a new CDC entry to simulate an update and a DELETE for the ID 1 and 2

# COMMAND ----------

# DBTITLE 1,Let's UPDATE id=1 and DELETE the row with id=2
# MAGIC %sql 
# MAGIC insert into clients_cdc  values ("Quentin", "Paris 75020", "quentin.ambard@databricks.com", 1, "UPDATE", now(), null);
# MAGIC insert into clients_cdc  values (null, null, null, 2, "DELETE", now(), null);
# MAGIC select * from clients_cdc where id in (1, 2);

# COMMAND ----------

# DBTITLE 1,Wait a few seconds for the stream to catch the new entry in the CDC table and check the results in the main table
# MAGIC %sql 
# MAGIC select * from retail_client_silver order by id asc;
# MAGIC -- Note that ID 1 has been updated, and ID 2 is deleted

# COMMAND ----------

# DBTITLE 1,The table history contains all our different versions in the silver table
# MAGIC %sql 
# MAGIC DESCRIBE HISTORY retail_client_silver;
# MAGIC -- If needed, we can go back in time to select a specific version or timestamp
# MAGIC -- SELECT * FROM retail_client_silver TIMESTAMP AS OF '2020-12-01'
# MAGIC 
# MAGIC -- And restore a given version
# MAGIC -- RESTORE retail_client_silver TO TIMESTAMP AS OF '2020-12-01'
# MAGIC 
# MAGIC -- Or clone the table (zero copy)
# MAGIC -- CREATE TABLE retail_client_silver_clone [SHALLOW | DEEP] CLONE retail_client_silver VERSION AS OF 32

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Capture and propagate Silver modifications downstream
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail-cdc-forecast/resources/images/cdc-flow-3.png" alt='Make all your data ready for BI and ML' style='float: right' width='600'/>
# MAGIC 
# MAGIC We need to add a final Gold layer based on the data from the Silver table. If a row is DELETED or UPDATED in the SILVER layer, we want to apply the same modification in the GOLD layer.
# MAGIC 
# MAGIC To do so, we need to capture all the tables changes from the SILVER layer and incrementally replicate the changes to the GOLD layer.
# MAGIC 
# MAGIC This is very simple using Delta Lake CDF from our SILVER table!
# MAGIC 
# MAGIC Delta Lake CDF provides the `table_changes('< table_name >', < delta_version >)` that you can use to select all the tables modifications from a specific Delta version to another one:

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Working with Delta Lake CDF

# COMMAND ----------

# MAGIC %sql
# MAGIC --Remember, CDC must be enabled in the silver table to capture the change. Let's make sure it's properly enabled:
# MAGIC ALTER TABLE retail_client_silver SET TBLPROPERTIES (delta.enableChangeDataCapture = true);
# MAGIC 
# MAGIC -- Delta Lake CDF works using table_changes function:
# MAGIC SELECT * FROM table_changes('retail_client_silver', 3)  order by id

# COMMAND ----------

# MAGIC %md #### Delta CDF table_changes output
# MAGIC Table Changes provides back 4 cdc types in the "_change_type" column:
# MAGIC 
# MAGIC | CDC Type             | Description                                                               |
# MAGIC |----------------------|---------------------------------------------------------------------------|
# MAGIC | **update_preimage**  | Content of the row before an update                                       |
# MAGIC | **update_postimage** | Content of the row after the update (what you want to capture downstream) |
# MAGIC | **delete**           | Content of a row that has been deleted                                    |
# MAGIC | **insert**           | Content of a new row that has been inserted                               |
# MAGIC 
# MAGIC Therefore, 1 update will result in 2 rows in the cdc stream (one row with the previous values, one with the new values)

# COMMAND ----------

# MAGIC %md #### Let's run some DELETE and UPDATE in our table to see the changes:
# MAGIC 
# MAGIC `table_changes` is also available with the Python API using the `readChangeData` option:

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE FROM retail_client_silver WHERE id = 1;
# MAGIC UPDATE retail_client_silver SET name='Marwa' WHERE id = 3;
# MAGIC select * from retail_client_silver where id in(1, 3)

# COMMAND ----------

# DBTITLE 1,Getting the last modification with the Python API
#Let's get the last table version to only see the last update mofications
last_version = str(DeltaTable.forName(spark, "retail_client_silver").history(1).head()["version"])
print(f"our Delta table last version is {last_version}, let's select the last changes to see our DELETE and UPDATE operations (last 2 versions):")

changes = spark.read.format("delta") \
                    .option("readChangeData", "true") \
                    .option("startingVersion", int(last_version) -1) \
                    .table("retail_client_silver")
display(changes)

# COMMAND ----------

# MAGIC %md ### Synchronizing our downstream GOLD table based from the Silver changes
# MAGIC Streaming operations with CDC are supported from DBR 8.1+

# COMMAND ----------

# DBTITLE 1,Let's create or final GOLD table: retail_client_gold
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS retail_client_gold (id BIGINT NOT NULL, name STRING, address STRING, email STRING, operation_date TIMESTAMP) USING delta;

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, regexp_replace

#Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDelta(data, batchId):
  #First we need to deduplicate based on the id and take the most recent update
  windowSpec = Window.partitionBy("id").orderBy(col("_commit_version").desc())
  #Select only the first value 
  #getting the latest change is still needed if the cdc contains multiple time the same id. We can rank over the id and get the most recent _commit_version
  data_deduplicated = data.withColumn("rank", rank().over(windowSpec)).where("rank = 1 and _change_type!='update_preimage'").drop("_commit_version", "rank")

  #Add some data cleaning for the gold layer to remove quotes from the address
  data_deduplicated = data_deduplicated.withColumn("address", regexp_replace(col("address"), "\"", ""))
  
  #run the merge in the gold table directly
  DeltaTable.forName(spark, "retail_client_gold").alias("target") \
      .merge(data_deduplicated.alias("source"), "source.id = target.id") \
      .whenMatchedDelete("source._change_type = 'delete'") \
      .whenMatchedUpdateAll("source._change_type != 'delete'") \
      .whenNotMatchedInsertAll("source._change_type != 'delete'") \
      .execute()


spark.readStream \
       .option("readChangeData", "true") \
       .option("startingVersion", 1) \
       .table("retail_client_silver") \
      .writeStream \
        .foreachBatch(upsertToDelta) \
      .start()

# COMMAND ----------

# MAGIC %sql SELECT * FROM retail_client_gold

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Support for data sharing and Datamesh organization
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/delta-cdf-datamesh.png" style="float:right; margin-right: 50px" width="300px" />
# MAGIC 
# MAGIC As we've seen during this demo, you can track all the changes (INSERT/UPDATE/DELETE) from any Detlta table using the CDC option.
# MAGIC 
# MAGIC It's then easy to subscribe the table modifications as an incremental process.
# MAGIC 
# MAGIC This makes the Data Mesh implementation easy: each Mesh can publish a set of tables, and other meshes can subscribe the original changes.
# MAGIC 
# MAGIC They are then in charge of propagating the changes (ex GDPR DELETE) to their own Data Mesh

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Data is now ready for BI & ML use-case !
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail-cdc-forecast/resources/images/cdc-flow-4.png" alt='Make all your data ready for BI and ML' style='float: right' width='600'/>
# MAGIC 
# MAGIC We now have our final table, updated based on the initial CDC information we receive.
# MAGIC 
# MAGIC As next step, we can leverage Databricks Lakehouse platform to start creating SQL queries / dashboards or ML models

# COMMAND ----------

# DBTITLE 1,Make sure we stop all actives streams
[s.stop() for s in spark.streams.active]
