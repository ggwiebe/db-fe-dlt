-- Databricks notebook source
-- MAGIC %md ### 0. Raw - Access Stream  
-- MAGIC   
-- MAGIC **Common Storage Format:** CloudFiles, Kafka, (non-DLT) Delta tables, etc.  

-- COMMAND ----------

-- -- RAW STREAM - View for new customers
-- CREATE INCREMENTAL LIVE VIEW customer_v
-- COMMENT "View built against raw, streaming Customer data source."
-- AS SELECT * FROM STREAM(ggw_retail.customer)

-- COMMAND ----------

-- MAGIC %md ### 1. BRONZE - Land Raw Data and standardize types
-- MAGIC   
-- MAGIC **Common Storage Format:** Delta  
-- MAGIC **Data Types:** Cast & check Nulls

-- COMMAND ----------

-- -- Done in Python to get proper schema inferencing
-- -- BRONZE - Read raw streaming file reader for "new" customer records
CREATE INCREMENTAL LIVE TABLE customer_bronze
  (
    id int COMMENT 'Casted to int',
    first_name string,
    last_name string,
    email string,
    active int,
    update_dt timestamp,
    update_user string
  )
TBLPROPERTIES ("quality" = "bronze")
COMMENT "New customer data incrementally ingested from cloud object storage landing zone"
AS 
SELECT 
    CAST(id AS int),
    first_name,
    last_name,
    email,
    CAST(active AS int),
    CAST(update_dt AS timestamp),
    update_user
  FROM cloud_files('/Users/glenn.wiebe@databricks.com/ggw_retail/data/in/', 'csv')

-- COMMAND ----------

-- MAGIC %md ### 2. SILVER - Cleansed Table
-- MAGIC   
-- MAGIC **Common Storage Format:** Delta  
-- MAGIC **Data Types:** Cast & check Nulls

-- COMMAND ----------

-- SILVER - Table based on stream of new customers
CREATE INCREMENTAL LIVE VIEW customer_bronze_clean_v (
  CONSTRAINT valid_id           EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_active       EXPECT (active BETWEEN 0 AND 1) ON VIOLATION DROP ROW,
  CONSTRAINT valid_first_name   EXPECT (first_name IS NOT NULL),
  CONSTRAINT valid_last_name    EXPECT (last_name IS NOT NULL)
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Cleansed bronze customer view (i.e. what will become Silver)"
AS SELECT id,
          UPPER(first_name) as first_name,
          UPPER(last_name) as last_name,
          email,
          active,
          current_timestamp() update_dt,
          current_user() update_user
     FROM STREAM(live.customer_bronze)

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE customer_silver
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean, merged customers"
;

-- COMMAND ----------

APPLY CHANGES INTO live.customer_silver
FROM stream(live.customer_bronze_clean_v)
  KEYS (id)
  APPLY AS DELETE WHEN active = 0
  SEQUENCE BY update_dt
;

-- COMMAND ----------

--   SEQUENCE BY id
--   COLUMNS * EXCEPT()
--   COLUMNS * EXCEPT (update_dt, update_user)
