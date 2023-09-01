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

single_schema = StructType(fields=[StructField("id", IntegerType(), False),
                                    StructField("name", StringType(), True),
                                    StructField("age", IntegerType(), True),
                                    StructField("email", StringType(), True)
])


single_standard = spark.read. \
    schema(single_schema)\
        .json("/mnt/adfdatalakeurbizedge/bronzeunitycatalog/single.json")


display(single_standard) # Display DataFrame


single_standard.createOrReplaceTempView("temp_view_single") #Create TempView 


%sql
select count(*)
from temp_view_single;
