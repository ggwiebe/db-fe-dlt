-- Databricks notebook source
%md  
  
+----+-----+------+
|user|movie|rating|
+----+-----+------+
|  11| 1753|     4|
|  11| 1682|     1|
|  11|  216|     4|
|  11| 2997|     4|
|  11| 1259|     3|

-- COMMAND ----------

ratings_pivot = ratings.groupBy("user")
                       .pivot("movie", popular.toSeq)
                       .agg(expr("coalesce(first(rating),3)")
                       .cast("double"))

