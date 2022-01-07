-- Databricks notebook source
-- MAGIC %md  
-- MAGIC   
-- MAGIC +----+-----+------+
-- MAGIC |user|movie|rating|
-- MAGIC +----+-----+------+
-- MAGIC |  11| 1753|     4|
-- MAGIC |  11| 1682|     1|
-- MAGIC |  11|  216|     4|
-- MAGIC |  11| 2997|     4|
-- MAGIC |  11| 1259|     3|

-- COMMAND ----------

ratings_pivot = ratings.groupBy("user")
                       .pivot("movie", popular.toSeq)
                       .agg(expr("coalesce(first(rating),3)")
                       .cast("double"))

