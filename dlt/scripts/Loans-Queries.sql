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

-- COMMAND ----------

-- Get details from Gold Load Balance Table #1
SELECT location_code,
       bal
  FROM ggw_loans.gl_total_loan_balances_1
 ORDER BY bal DESC
;

-- COMMAND ----------

-- Get details from Gold Load Balance Table #2
SELECT location_code,
       bal
  FROM ggw_loans.gl_total_loan_balances_2
 ORDER BY bal DESC
;
