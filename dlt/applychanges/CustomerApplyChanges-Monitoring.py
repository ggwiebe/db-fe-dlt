# Databricks notebook source
# MAGIC %md # Delta Live Tables - Monitoring

# COMMAND ----------

dbutils.widgets.text('root_location', '/Users/glenn.wiebe@databricks.com/')
dbutils.widgets.text('db_name', 'ggw_retail')
dbutils.widgets.text('data_loc','/data')
dbutils.widgets.text('storage_loc','/dlt_storage')
# -- REMOVE WIDGET old

# COMMAND ----------

root_location = dbutils.widgets.get('root_location')
db_name       = dbutils.widgets.get('db_name')
data_loc      = dbutils.widgets.get('data_loc')
storage_loc   = dbutils.widgets.get('storage_loc')
storage_path  = root_location + db_name + storage_loc

print('root_location: {}\ndb_name:       {}\ndata_loc:      {}\nstorage_loc:   {}'.format(root_location,db_name,data_loc,storage_loc))
print('storage_path:  {}'.format(storage_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS $db_name.event_log
# MAGIC  USING delta
# MAGIC LOCATION '$root_location$db_name$storage_loc/system/events'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC        id,
# MAGIC        timestamp,
# MAGIC        sequence,
# MAGIC        -- origin,
# MAGIC        event_type,
# MAGIC        message,
# MAGIC        level, 
# MAGIC        -- error ,
# MAGIC        details
# MAGIC   FROM $db_name.event_log
# MAGIC  ORDER BY timestamp ASC
# MAGIC ;  

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP VIEW customers_raw;
# MAGIC CREATE OR REPLACE VIEW ggw_retail.customers_raw 
# MAGIC --    (
# MAGIC --       id int, first_name string, last_name string, email string, active int, update_dt timestamp, update_user string
# MAGIC --    )
# MAGIC  USING CSV
# MAGIC OPTIONS (
# MAGIC     path "/Users/glenn.wiebe@databricks.com/ggw_retail/data/*.csv",
# MAGIC     header "true",
# MAGIC --     inferSchema "true",
# MAGIC --     mode "FAILFAST",
# MAGIC     schema 'id int, first_name string, last_name string, email string, active int, update_dt int, update_user string'
# MAGIC   )
# MAGIC ;

# COMMAND ----------

-- Create or replace view for `experienced_employee` with comments.
> CREATE OR REPLACE VIEW experienced_employee
    (id COMMENT 'Unique identification number', Name)
    COMMENT 'View for experienced employees'
    AS SELECT id, name
         FROM all_employee
        WHERE working_years > 5;

-- Create a temporary view `subscribed_movies` if it does not exist.
> CREATE TEMPORARY VIEW IF NOT EXISTS subscribed_movies
    AS SELECT mo.member_id, mb.full_name, mo.movie_title
         FROM movies AS mo
         INNER JOIN members AS mb
            ON mo.member_id = mb.id;
