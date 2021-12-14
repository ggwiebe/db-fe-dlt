-- Databricks notebook source

(sql("""select *, concat('Q', d_qoy) as qoy
  from store_sales
  join date_dim on ss_sold_date_sk = d_date_sk
  join item on ss_item_sk = i_item_sk""")
  .groupBy("i_category")
  .pivot("qoy")
  .agg(round(sum("ss_sales_price")/1000000,2))
  .show)

-- COMMAND ----------

%md ## Results from query should look like this:  
  
```
+-----------+----+----+----+----+
| i_category|  Q1|  Q2|  Q3|  Q4|
+-----------+----+----+----+----+
|      Books|1.58|1.50|2.84|4.66|
|      Women|1.41|1.36|2.54|4.16|
|      Music|1.50|1.44|2.66|4.36|
|   Children|1.54|1.46|2.74|4.51|
|     Sports|1.47|1.40|2.62|4.30|
|      Shoes|1.51|1.48|2.68|4.46|
|    Jewelry|1.45|1.39|2.59|4.25|
|       null|0.04|0.04|0.07|0.13|
|Electronics|1.56|1.49|2.77|4.57|
|       Home|1.57|1.51|2.79|4.60|
|        Men|1.60|1.54|2.86|4.71|
+-----------+----+----+----+----+
```