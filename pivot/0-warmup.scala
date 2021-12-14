-- Databricks notebook source

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("0-Warmup/table.csv")
df.groupBy("A", "B").pivot("C").sum("D").show