# Databricks notebook source
#To see all your mount folders
display(dbutils.fs.mounts())

# COMMAND ----------

#Read JSON file using spark.read()
singlejson_df = spark.read.json("/mnt/adfdatalakeurbizedge/bronzeunitycatalog/single.json")

# COMMAND ----------

display(singlejson_df)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
