-- Databricks notebook source
CREATE LIVE TABLE source AS SELECT 
col1 AS userId,
col2 AS name,
col3 AS city,
col4 AS operation,
col5 AS sequenceNum
FROM (VALUES 
--Initial load.
(123, "Isabel", "Monterrey", "Insert", 1),
(124, "Raul", "Oaxaca", "INSERT", 1),
--one new user.
(125, "Mercedes", "Tijuana", "INSERT", 2),
-- Isabel got removed from the system and Merche moved to Guadalajara.
(123, null, null, "DELETE", 5),
(125, "Mercedes", "Guadalajara", "UPDATE", 5),
-- This batch of updates came out of order - the above batch at sequenceNum 5 will be the final state.
(123, "Isabel", "Chihuahua", "UPDATE", 4),
(125, "Mercedes", "Mexicali", "UPDATE", 4));

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE target;

-- COMMAND ----------

APPLY CHANGES INTO live.target
FROM stream(live.source)
  KEYS (userId)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation, sequenceNum);

-- COMMAND ----------


