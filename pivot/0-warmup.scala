// Databricks notebook source
val df = sqlContext.read.format("csv")
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .load("file:/Workspace/Repos/glenn.wiebe@databricks.com/db-fe-dlt/pivot/data/table.csv")


// COMMAND ----------

display(df)

// COMMAND ----------

val pivotDF = (df.groupBy("A", "B")
                 .pivot("C")
                 .sum("D")
              )

// COMMAND ----------

display(pivotDF)
