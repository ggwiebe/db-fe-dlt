-- Databricks notebook source
-- Get head of raw transactions
SELECT *
  FROM ggw_loans.bz_raw_txs
LIMIT 100
;

-- COMMAND ----------

-- Get details of count of raw transactions
SELECT COUNT(*)
  FROM ggw_loans.bz_raw_txs
;
