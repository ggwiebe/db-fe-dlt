-- Databricks notebook source

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
#Create spark session
data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)


-- COMMAND ----------

pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.printSchema()
pivotDF.show(truncate=False)


countries = ["USA","China","Canada","Mexico"]
pivotDF = df.groupBy("Product")
            .pivot("Country", countries)
            .sum("Amount")
pivotDF.show(truncate=False)

-- COMMAND ----------

# Unpivot
from pyspark.sql.functions import expr
unpivotExpr = "stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)"
unPivotDF = pivotDF.select("Product", expr(unpivotExpr)) \
    .where("Total is not null")
unPivotDF.show(truncate=False)
unPivotDF.show()

-- COMMAND ----------

# Pivot without aggregation
# scala?
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

//creating schema from existing dataframe
val schema = StructType(df.select(collect_list("COLUMN_NAME"))
                          .first().getAs[Seq[String]](0)
                          .map(x => StructField(x, StringType)))

//creating RDD[Row] 
val values = sc.parallelize(Seq(
                                 Row.fromSeq(
                                     df.select(collect_list("VALUE"))
                                       .first().getAs[Seq[String]](0)
                                 )
                               )
                           )

//new dataframe creation
sqlContext.createDataFrame(values, schema).show(false)

-- COMMAND ----------

